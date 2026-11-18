use anyhow::{Context, Result, bail};
use openssl::{
    asn1::Asn1Time,
    bn::{BigNum, MsbOption},
    hash::MessageDigest,
    pkey::{PKey, Private},
    rsa::Rsa,
    x509::{
        extension::{
            AuthorityKeyIdentifier, BasicConstraints, ExtendedKeyUsage, KeyUsage,
            SubjectAlternativeName, SubjectKeyIdentifier,
        },
        X509NameBuilder, X509,
    },
};
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

const CA_CERT_FILE: &str = "fastgithub.crt";
const CA_KEY_FILE: &str = "fastgithub.key";
const RSA_BITS: u32 = 2048;
const DAY_SECONDS: u64 = 24 * 60 * 60;

/// A generated leaf certificate and its private key.
pub struct ServerCertificate {
    pub certificate: X509,
    pub private_key: PKey<Private>,
}

/// Persistent FastGithub CA plus an in-memory per-host leaf-certificate cache.
///
/// The CA private key is written with mode 0600 on Unix. This service deliberately
/// does not install the CA into the operating-system trust store.
pub struct CertificateService {
    directory: PathBuf,
    ca_certificate: X509,
    ca_private_key: PKey<Private>,
    server_certificates: Mutex<HashMap<String, Arc<ServerCertificate>>>,
}

impl CertificateService {
    pub fn load_or_create(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)
            .with_context(|| format!("creating certificate directory {}", directory.display()))?;
        let cert_path = directory.join(CA_CERT_FILE);
        let key_path = directory.join(CA_KEY_FILE);

        let (ca_certificate, ca_private_key) = if cert_path.is_file() && key_path.is_file() {
            let certificate = X509::from_pem(&fs::read(&cert_path)?)
                .with_context(|| format!("loading CA certificate {}", cert_path.display()))?;
            let private_key = PKey::private_key_from_pem(&fs::read(&key_path)?)
                .with_context(|| format!("loading CA private key {}", key_path.display()))?;
            if !certificate.public_key()?.public_eq(&private_key) {
                bail!("FastGithub CA certificate and private key do not match");
            }
            (certificate, private_key)
        } else {
            // Never leave a half-pair that could be mistaken for a valid CA on next start.
            let _ = fs::remove_file(&cert_path);
            let _ = fs::remove_file(&key_path);
            let (certificate, private_key) = create_ca()?;
            write_atomic(&cert_path, &certificate.to_pem()?, false)?;
            write_atomic(&key_path, &private_key.private_key_to_pem_pkcs8()?, true)?;
            (certificate, private_key)
        };

        Ok(Self {
            directory,
            ca_certificate,
            ca_private_key,
            server_certificates: Mutex::new(HashMap::new()),
        })
    }

    pub fn ca_certificate_path(&self) -> PathBuf {
        self.directory.join(CA_CERT_FILE)
    }

    pub fn ca_private_key_path(&self) -> PathBuf {
        self.directory.join(CA_KEY_FILE)
    }

    pub fn ca_certificate(&self) -> &X509 {
        &self.ca_certificate
    }

    pub fn server_certificate(&self, host: &str) -> Result<Arc<ServerCertificate>> {
        let cache_key = host.trim_end_matches('.').to_ascii_lowercase();
        if cache_key.is_empty() {
            bail!("cannot issue a certificate for an empty host");
        }

        let mut cache = self.server_certificates.lock()
            .map_err(|_| anyhow::anyhow!("server certificate cache lock poisoned"))?;
        if let Some(certificate) = cache.get(&cache_key) {
            return Ok(Arc::clone(certificate));
        }

        let certificate = Arc::new(create_server_certificate(
            &self.ca_certificate,
            &self.ca_private_key,
            &cache_key,
        )?);
        cache.insert(cache_key, Arc::clone(&certificate));
        Ok(certificate)
    }
}

fn create_ca() -> Result<(X509, PKey<Private>)> {
    let key = PKey::from_rsa(Rsa::generate(RSA_BITS)?)?;
    let mut name = X509NameBuilder::new()?;
    name.append_entry_by_text("CN", "FastGithub")?;
    let name = name.build();

    let now = unix_time()?;
    let mut builder = X509::builder()?;
    builder.set_version(2)?;
    builder.set_serial_number(&serial_number()?)?;
    builder.set_subject_name(&name)?;
    builder.set_issuer_name(&name)?;
    builder.set_pubkey(&key)?;
    builder.set_not_before(&Asn1Time::from_unix(now - DAY_SECONDS as i64)?)?;
    builder.set_not_after(&Asn1Time::from_unix(now + 10 * 365 * DAY_SECONDS as i64)?)?;
    builder.append_extension(BasicConstraints::new().critical().ca().pathlen(1).build()?)?;
    builder.append_extension(
        KeyUsage::new().critical().digital_signature().key_cert_sign().crl_sign().build()?,
    )?;
    builder.append_extension(ExtendedKeyUsage::new().server_auth().client_auth().build()?)?;

    let context = builder.x509v3_context(None, None);
    let subject_key_id = SubjectKeyIdentifier::new().build(&context)?;
    let subject_alt_name = SubjectAlternativeName::new().dns("FastGithub").build(&context)?;
    drop(context);
    builder.append_extension(subject_key_id)?;
    builder.append_extension(subject_alt_name)?;
    builder.sign(&key, MessageDigest::sha256())?;
    Ok((builder.build(), key))
}

fn create_server_certificate(
    ca_certificate: &X509,
    ca_private_key: &PKey<Private>,
    host: &str,
) -> Result<ServerCertificate> {
    let key = PKey::from_rsa(Rsa::generate(RSA_BITS)?)?;
    let mut name = X509NameBuilder::new()?;
    name.append_entry_by_text("CN", host)?;
    let name = name.build();

    let now = unix_time()?;
    let mut builder = X509::builder()?;
    builder.set_version(2)?;
    builder.set_serial_number(&serial_number()?)?;
    builder.set_subject_name(&name)?;
    builder.set_issuer_name(ca_certificate.subject_name())?;
    builder.set_pubkey(&key)?;
    builder.set_not_before(&Asn1Time::from_unix(now - DAY_SECONDS as i64)?)?;
    builder.set_not_after(&Asn1Time::from_unix(now + 365 * DAY_SECONDS as i64)?)?;
    builder.append_extension(BasicConstraints::new().critical().build()?)?;
    builder.append_extension(
        KeyUsage::new().critical().digital_signature().key_encipherment().build()?,
    )?;
    builder.append_extension(ExtendedKeyUsage::new().server_auth().client_auth().build()?)?;

    let context = builder.x509v3_context(Some(ca_certificate), None);
    let authority_key_id = AuthorityKeyIdentifier::new().keyid(true).issuer(true).build(&context)?;
    let subject_key_id = SubjectKeyIdentifier::new().build(&context)?;
    let mut san = SubjectAlternativeName::new();
    add_san_name(&mut san, host);
    let machine_name = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_owned());
    if !machine_name.eq_ignore_ascii_case(host) {
        add_san_name(&mut san, &machine_name);
    }
    san.ip("127.0.0.1").ip("::1");
    let subject_alt_name = san.build(&context)?;
    drop(context);

    builder.append_extension(authority_key_id)?;
    builder.append_extension(subject_key_id)?;
    builder.append_extension(subject_alt_name)?;
    builder.sign(ca_private_key, MessageDigest::sha256())?;

    Ok(ServerCertificate {
        certificate: builder.build(),
        private_key: key,
    })
}

fn add_san_name(san: &mut SubjectAlternativeName, name: &str) {
    if name.parse::<IpAddr>().is_ok() {
        san.ip(name);
    } else {
        san.dns(name);
    }
}

fn serial_number() -> Result<openssl::asn1::Asn1Integer> {
    let mut serial = BigNum::new()?;
    serial.rand(128, MsbOption::MAYBE_ZERO, false)?;
    serial.to_asn1_integer().context("creating certificate serial number")
}

fn unix_time() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64)
}

fn write_atomic(path: &Path, contents: &[u8], secret: bool) -> Result<()> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(if secret { 0o600 } else { 0o644 });
    }
    let mut file = options.open(&temporary)
        .with_context(|| format!("creating {}", temporary.display()))?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)
        .with_context(|| format!("installing {}", path.display()))?;
    Ok(())
}
