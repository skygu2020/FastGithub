use anyhow::{bail, Context, Result};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

fn run_systemctl(args: &[&str]) -> Result<()> {
    let status = Command::new("systemctl")
        .args(args)
        .status()
        .context("starting systemctl; is systemd installed?")?;
    if !status.success() {
        bail!("systemctl {} failed with {status}", args.join(" "));
    }
    Ok(())
}

pub async fn install_and_start(executable: &Path) -> Result<()> {
    if unsafe { libc_geteuid() } != 0 {
        bail!("root privileges are required to install a system service");
    }

    let executable = executable.canonicalize()?;
    let working_dir = executable.parent().context("binary has no parent directory")?;
    let unit_name = "fastgithub-rs.service";
    let unit_path = Path::new("/etc/systemd/system").join(unit_name);
    let unit = format!(
        "[Unit]\nDescription=FastGithub (Rust)\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nExecStart={}\nWorkingDirectory={}\nRestart=on-failure\nRestartSec=2\n\n[Install]\nWantedBy=multi-user.target\n",
        executable.display(),
        working_dir.display()
    );
    fs::write(&unit_path, unit).with_context(|| format!("writing {}", unit_path.display()))?;
    fs::set_permissions(&unit_path, fs::Permissions::from_mode(0o644))?;
    run_systemctl(&["daemon-reload"])?;
    run_systemctl(&["enable", "--now", unit_name])?;
    tracing::info!("installed and started {unit_name}");
    Ok(())
}

pub async fn stop_and_remove(_executable: &Path) -> Result<()> {
    if unsafe { libc_geteuid() } != 0 {
        bail!("root privileges are required to remove the system service");
    }
    let unit_name = "fastgithub-rs.service";
    let unit_path = Path::new("/etc/systemd/system").join(unit_name);
    let _ = run_systemctl(&["disable", "--now", unit_name]);
    if unit_path.exists() {
        fs::remove_file(&unit_path)?;
    }
    run_systemctl(&["daemon-reload"])?;
    tracing::info!("stopped and removed {unit_name}");
    Ok(())
}

unsafe fn libc_geteuid() -> u32 {
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    geteuid()
}
