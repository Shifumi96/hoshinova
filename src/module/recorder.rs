use super::{Message, Module, Notification, Task, TaskStatus};
use crate::msgbus::BusTx;
use crate::{config::Config, module::RecordingStatus};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use serde::Serialize;
use std::collections::HashSet;
use std::{
    fs,
    path::Path,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, BufReader},
    sync::{mpsc, RwLock},
};
use ts_rs::TS;

pub struct YtDlp {
    config: Arc<RwLock<Config>>,
    active_ids: Arc<RwLock<HashSet<String>>>,
}

impl YtDlp {
    async fn record(cfg: Config, task: Task, bus: &mut BusTx<Message>) -> Result<()> {
        let task_name = format!("[{}][{}][{}]", task.video_id, task.channel_name, task.title);

        // Ensure the working directory exists
        let cfg = &cfg.ytdlp;
        tokio::fs::create_dir_all(&cfg.working_directory)
            .await
            .context("Failed to create working directory")?;

        // Ensure the output directory exists
        tokio::fs::create_dir_all(&task.output_directory)
            .await
            .context("Failed to create output directory")?;

        // Construct the command line arguments
        let mut args = cfg.args.clone();

        // Add --wait-for-video if not present
        if !args.iter().any(|a| a == "--wait-for-video") {
            args.push("--wait-for-video".to_string());
            args.push("30".to_string());
        }

        // Cookies for member-only streams
        if let Some(ref path) = cfg.cookies_file {
            if !args.iter().any(|a| a == "--cookies") {
                // Resolve to absolute path so it works regardless of yt-dlp's working_directory
                let abs_cookies = std::fs::canonicalize(path)
                    .unwrap_or_else(|_| std::path::PathBuf::from(path));
                args.push("--cookies".to_string());
                args.push(abs_cookies.to_string_lossy().to_string());
            }
        }

        // Add format selector
        if !cfg.format.is_empty() && !args.iter().any(|a| a == "-f" || a == "--format") {
            args.push("-f".to_string());
            args.push(cfg.format.clone());
        }

        // URL last
        args.push(format!("https://youtu.be/{}", task.video_id));

        // Start the process
        debug!("{} Starting yt-dlp with args {:?}", task_name, args);
        let mut process = tokio::process::Command::new(&cfg.executable_path)
            .args(args)
            .current_dir(&cfg.working_directory)
            // Disable Python output buffering so we get lines in real time
            .env("PYTHONUNBUFFERED", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to start yt-dlp")?;;

        // Grab stdout/stderr byte iterators
        let mut stdout = BufReader::new(
            process
                .stdout
                .take()
                .ok_or(anyhow!("Failed to take stdout"))?,
        );
        let mut stderr = BufReader::new(
            process
                .stderr
                .take()
                .ok_or(anyhow!("Failed to take stderr"))?,
        );

        // Create a channel to consolidate stdout and stderr
        let (tx, mut rx) = mpsc::channel(1);

        // Flag to mark when the process has exited
        let done = Arc::from(AtomicBool::new(false));

        macro_rules! read_line {
            ($reader:expr, $tx:expr) => {{
                // Read bytes until a \r or \n is returned
                let mut bytes = Vec::new();
                loop {
                    match $reader.read_u8().await {
                        Ok(byte) => {
                            if byte == b'\r' || byte == b'\n' {
                                break;
                            }
                            bytes.push(byte);
                        }
                        _ => break,
                    }
                }

                // Skip if there are no bytes
                if bytes.is_empty() {
                    continue;
                }

                // Convert to a string
                let line = match std::str::from_utf8(&bytes) {
                    Ok(line) => line.to_owned(),
                    Err(e) => {
                        trace!("Failed to read utf8: {:?}", e);
                        break;
                    }
                };

                // Send the line to the channel
                if let Err(e) = $tx.send(line).await {
                    trace!("Failed to send line: {:?}", e);
                    break;
                }
            }};
        }

        // Read stdout
        let h_stdout = tokio::spawn({
            let done = done.clone();
            let task_name = task_name.clone();
            let tx = tx.clone();
            async move {
                while !done.load(Ordering::Relaxed) {
                    read_line!(&mut stdout, tx);
                }
                trace!("{} stdout reader exited", task_name);
            }
        });

        // Read stderr
        let h_stderr = tokio::spawn({
            let done = done.clone();
            let task_name = task_name.clone();
            let tx = tx.clone();
            async move {
                while !done.load(Ordering::Relaxed) {
                    read_line!(&mut stderr, tx);
                }
                trace!("{} stderr reader exited", task_name);
            }
        });

        // Wait for the process to exit
        let h_wait = tokio::spawn({
            let done = done.clone();
            let task_name = task_name.clone();
            async move {
                let result = process.wait().await;

                // Wait a bit for the stdout to be completely read
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                // Stop threads
                done.store(true, Ordering::Relaxed);
                debug!("{} Process exited with {:?}", task_name, result);

                // Send a blank message to unblock the status monitor thread
                let _ = tx.send("".into());

                result
            }
        });

        // Parse each line
        let mut status = YTAStatus::new();

        // Send an initial status so the UI shows the task immediately
        let _ = bus
            .send(Message::RecordingStatus(RecordingStatus {
                task: task.clone(),
                status: status.clone(),
            }))
            .await;

        loop {
            let line = match rx.recv().await {
                Some(line) => line,
                None => break,
            };

            // Stop when done
            if done.load(Ordering::Relaxed) {
                break;
            }

            trace!("{}[ytdlp:out] {}", task_name, line);

            let old = status.clone();
            status.parse_line(&line);

            // Push the current status to the bus
            if let Err(_) = bus
                .send(Message::RecordingStatus(RecordingStatus {
                    task: task.clone(),
                    status: status.clone(),
                }))
                .await
            {
                break;
            }

            // Check if status changed
            if old.state == status.state {
                continue;
            }

            let message = match status.state {
                YTAState::Waiting(_) => {
                    info!("{} Waiting for stream to go live", task_name);
                    // Only notify on the first Waiting transition (from Idle).
                    // Subsequent AlreadyProcessed → Waiting cycles (stream still
                    // processing on YouTube's side) should not spam Discord.
                    if old.state == YTAState::Idle {
                        Some(Message::ToNotify(Notification {
                            task: task.clone(),
                            status: TaskStatus::Waiting,
                        }))
                    } else {
                        None
                    }
                }
                YTAState::Recording => {
                    info!("{} Recording started", task_name);
                    Some(Message::ToNotify(Notification {
                        task: task.clone(),
                        status: TaskStatus::Recording,
                    }))
                }
                YTAState::Muxing => {
                    info!("{} Muxing", task_name);
                    None
                }
                YTAState::Finished => {
                    info!("{} Recording finished", task_name);
                    Some(Message::ToNotify(Notification {
                        task: task.clone(),
                        status: TaskStatus::Done,
                    }))
                }
                YTAState::AlreadyProcessed => {
                    info!("{} Video already processed, skipping", task_name);
                    None
                }
                YTAState::Interrupted => {
                    info!("{} Recording interrupted", task_name);
                    Some(Message::ToNotify(Notification {
                        task: task.clone(),
                        status: TaskStatus::Failed,
                    }))
                }
                YTAState::Errored => {
                    info!("{} Recording failed", task_name);
                    Some(Message::ToNotify(Notification {
                        task: task.clone(),
                        status: TaskStatus::Failed,
                    }))
                }
                _ => None,
            };

            if let Some(message) = message {
                // Exit the loop if message failed to send
                if let Err(_) = bus.send(message).await {
                    break;
                }
            }
        }

        trace!("{} Status loop exited: {:?}", task_name, status);

        // Wait for threads to finish
        let (r_wait, r_stdout, r_stderr) = futures::join!(h_wait, h_stdout, h_stderr);
        trace!("{} Process monitor exited: {:?}", task_name, r_wait);
        trace!("{} Stdout monitor quit: {:?}", task_name, r_stdout);
        trace!("{} Stderr monitor quit: {:?}", task_name, r_stderr);

        // Check process exit code
        let exit_ok = r_wait
            .ok()
            .and_then(|r| r.ok())
            .map(|s| s.success())
            .unwrap_or(false);

        // Mark as finished when process exits successfully, or when muxing
        // completed even if yt-dlp returned a non-zero exit code (e.g. due to
        // skipped fragments that don't prevent a usable output file).
        if (exit_ok
            && matches!(
                status.state,
                YTAState::Recording | YTAState::Muxing | YTAState::Idle
            ))
            || status.state == YTAState::Muxing
        {
            status.state = YTAState::Finished;
            let _ = bus
                .send(Message::ToNotify(Notification {
                    task: task.clone(),
                    status: TaskStatus::Done,
                }))
                .await;
        }

        // Push final status to the UI
        let _ = bus
            .send(Message::RecordingStatus(RecordingStatus {
                task: task.clone(),
                status: status.clone(),
            }))
            .await;

        // Skip moving files if it didn't finish
        if status.state != YTAState::Finished {
            return Ok(());
        }

        // Move the video to the output directory
        let frompath_str = status.output_file;
        // Resolve the captured path, then fall back to scanning by video ID
        let frompath_buf = if let Some(ref s) = frompath_str {
            let candidate = if Path::new(s).is_absolute() {
                std::path::PathBuf::from(s)
            } else {
                Path::new(&cfg.working_directory).join(s)
            };
            if candidate.exists() {
                Some(candidate)
            } else {
                warn!("{} Output file not found at {:?}, searching by video ID", task_name, candidate);
                None
            }
        } else {
            warn!("{} yt-dlp did not emit an output file path, searching by video ID", task_name);
            None
        };

        // Fallback: scan working directory for a file containing the video ID
        let frompath_buf = if let Some(buf) = frompath_buf {
            buf
        } else {
            let mut found = None;
            if let Ok(entries) = fs::read_dir(&cfg.working_directory) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy().to_string();
                    if name_str.contains(&task.video_id)
                        && !name_str.ends_with(".part")
                        && !name_str.contains(".frag")
                        && !name_str.ends_with(".ytdl")
                    {
                        found = Some(entry.path());
                        break;
                    }
                }
            }
            found.ok_or_else(|| anyhow!("Could not find output file for video {}", task.video_id))?
        };

        let frompath = frompath_buf.as_path();
        debug!("{} Moving output file from {:?}", task_name, frompath);
        let filename = frompath
            .file_name()
            .ok_or(anyhow!("Failed to get filename"))?;
        let destpath = Path::new(&task.output_directory).join(filename);

        // Try to rename the file into the output directory
        if let Err(_) = fs::rename(frompath, &destpath) {
            debug!(
                "{} Failed to rename file to output, trying to copy",
                task_name,
            );

            // Copy the file into the output directory
            fs::copy(frompath, &destpath)
                .with_context(|| format!("Failed to copy file to output: {:?}", destpath))?;
            info!(
                "{} Copied output file to {}, removing original",
                task_name,
                destpath.display(),
            );
            fs::remove_file(frompath)
                .with_context(|| format!("Failed to remove original file: {:?}", frompath))?;
        }

        info!("{} Moved output file to {}", task_name, destpath.display());
        Ok(())
    }
}

struct SpawnTask {
    task: Task,
    cfg: Config,
    tx: BusTx<Message>,
}

#[async_trait]
impl Module for YtDlp {
    fn new(config: Arc<RwLock<Config>>) -> Self {
        let active_ids = Arc::new(RwLock::new(HashSet::new()));
        Self { config, active_ids }
    }

    async fn run(&self, tx: &BusTx<Message>, rx: &mut mpsc::Receiver<Message>) -> Result<()> {
        // Create a spawn queue
        let (spawn_tx, mut spawn_rx) = mpsc::unbounded_channel::<SpawnTask>();

        // Future to handle spawning new tasks
        let active_ids = self.active_ids.clone();
        let f_spawner = async move {
            while let Some(mut task) = spawn_rx.recv().await {
                let active_ids = active_ids.clone();
                let delay = task.cfg.ytdlp.delay_start;

                debug!("Spawning yt-dlp thread for task: {:?}", task.task);
                tokio::spawn(async move {
                    let video_id = task.task.video_id.clone();
                    active_ids.write().await.insert(video_id.clone());

                    if let Err(e) = YtDlp::record(task.cfg, task.task, &mut task.tx).await {
                        error!("Failed to record task: {:?}", e);
                    };

                    active_ids.write().await.remove(&video_id);
                });

                // Wait a bit before starting the next task
                tokio::time::sleep(delay).await;
            }

            Ok::<(), anyhow::Error>(())
        };

        // Future to handle incoming messages
        let f_message = async move {
            while let Some(message) = rx.recv().await {
                match message {
                    Message::ToRecord(task) => {
                        // Check if the task is already active
                        if self.active_ids.read().await.contains(&task.video_id) {
                            warn!("Task {} is already active, skipping", task.video_id);
                            continue;
                        }

                        debug!("Adding task to spawn queue: {:?}", task);
                        let tx = tx.clone();
                        let cfg = self.config.read().await.clone();

                        if let Err(_) = spawn_tx.send(SpawnTask { task, cfg, tx }) {
                            debug!("Spawn queue closed, exiting");
                            break;
                        }
                    }
                    _ => (),
                }
            }

            Ok::<(), anyhow::Error>(())
        };

        // Run the futures
        tokio::try_join!(f_spawner, f_message)?;

        debug!("YtDlp module finished");
        Ok(())
    }
}

/// The current recording status.
#[derive(Debug, Clone, TS, Serialize)]
#[ts(export, export_to = "web/src/bindings/")]
pub struct YTAStatus {
    version: Option<String>,
    state: YTAState,
    last_output: Option<String>,
    last_update: chrono::DateTime<chrono::Utc>,
    video_fragments: Option<u32>,
    audio_fragments: Option<u32>,
    total_size: Option<String>,
    video_quality: Option<String>,
    output_file: Option<String>,
}

#[derive(Debug, Clone, PartialEq, TS, Serialize)]
#[ts(export, export_to = "web/src/bindings/")]
pub enum YTAState {
    Idle,
    Waiting(Option<DateTime<Utc>>),
    Recording,
    Muxing,
    Finished,
    AlreadyProcessed,
    Interrupted,
    Errored,
}

fn strip_ansi(s: &str) -> String {
    lazy_static! {
        static ref RE: Regex = Regex::new(concat!(
            r"[\u001B\u009B][[\\]()#;?]*",
            r"(?:(?:(?:[a-zA-Z\\d]*(?:;[a-zA-Z\\d]*)*)?\u0007)|",
            r"(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PRZcf-ntqry=><~]))",
        ))
        .expect("Failed to compile ANSI stripping regex");
    }
    let stripped = RE.replace_all(s, "").to_string();
    stripped
        .strip_suffix("\u{001b}[K")
        .unwrap_or(&stripped)
        .to_string()
}

impl YTAStatus {
    pub fn new() -> Self {
        Self {
            version: None,
            state: YTAState::Idle,
            last_output: None,
            last_update: chrono::Utc::now(),
            video_fragments: None,
            audio_fragments: None,
            total_size: None,
            video_quality: None,
            output_file: None,
        }
    }

    /// parse_line parses a line of output from the yt-dlp process.
    ///
    /// Sample output:
    ///
    ///   [youtube] VIDEO_ID: Waiting for VIDEO_ID - expected start time is ...
    ///   [youtube] VIDEO_ID: Waiting for 30 seconds
    ///   [hlsFfmpeg] Destination: temp/20220314 Title [Channel] (VIDEO_ID).mkv
    ///   [download] 1 fragments downloaded (2.34MiB)
    ///   [Merger] Merging formats into "output.mkv"
    ///   ERROR: ...
    pub fn parse_line(&mut self, line: &str) {
        self.last_output = Some(line.to_string());
        self.last_update = chrono::Utc::now();

        let line = strip_ansi(line);

        lazy_static! {
            static ref FRAGMENT_RE: Regex =
                Regex::new(r"\[download\] (\d+) fragments? downloaded \(([^)]+)\)")
                    .expect("Failed to compile yt-dlp fragment regex");
            static ref CONCURRENT_PROGRESS_RE: Regex =
                Regex::new(r"^\d+: \[download\]\s+([\d.]+\S+) at")
                    .expect("Failed to compile yt-dlp concurrent progress regex");
            static ref WAITING_TIME_RE: Regex =
                Regex::new(r"expected start time is (\S+)")
                    .expect("Failed to compile yt-dlp waiting time regex");
            static ref FORMAT_RE: Regex =
                Regex::new(r"\[info\] \S+: Downloading \d+ format\(s\): (.+)")
                    .expect("Failed to compile yt-dlp format regex");
        }

        if line.contains("[youtube]") && line.contains("Waiting for") {
            let date = WAITING_TIME_RE
                .captures(&line)
                .and_then(|c| c.get(1))
                .and_then(|m| DateTime::parse_from_rfc3339(m.as_str()).ok())
                .map(|d| d.into());
            self.state = YTAState::Waiting(date);
        } else if let Some(stripped) = line.strip_prefix("[hlsFfmpeg] Destination: ") {
            // HLS download: capture output filename
            self.state = YTAState::Recording;
            self.output_file = Some(stripped.trim().to_string());
        } else if let Some(stripped) = line.strip_prefix("[download] Destination: ") {
            // DASH/direct download: track the last destination (merged file comes last)
            self.state = YTAState::Recording;
            self.output_file = Some(stripped.trim().to_string());
        } else if line.starts_with("[Merger] Merging formats into \"") {
            self.state = YTAState::Muxing;
            let filename = line
                .trim_start_matches("[Merger] Merging formats into \"")
                .trim_end_matches('"');
            self.output_file = Some(filename.to_string());
        } else if let Some(caps) = CONCURRENT_PROGRESS_RE.captures(&line) {
            // Concurrent fragment worker progress: "1: [download] 1.29GiB at ..."
            self.state = YTAState::Recording;
            if let Some(s) = caps.get(1) {
                self.total_size = Some(s.as_str().to_string());
            }
        } else if let Some(caps) = FRAGMENT_RE.captures(&line) {
            self.state = YTAState::Recording;
            if let Some(n) = caps.get(1).and_then(|m| m.as_str().parse().ok()) {
                self.video_fragments = Some(n);
            }
            if let Some(s) = caps.get(2) {
                self.total_size = Some(s.as_str().to_string());
            }
        } else if line.starts_with("[download] Downloading fragment") {
            self.state = YTAState::Recording;
        } else if let Some(caps) = FORMAT_RE.captures(&line) {
            self.video_quality = Some(caps[1].to_string());
        } else if line.starts_with("ERROR:") {
            // Only error out if we haven't started recording/muxing yet.
            // Non-fatal errors (e.g. skipped fragments) during download should
            // not abort a recording that is otherwise progressing.
            if !matches!(self.state, YTAState::Recording | YTAState::Muxing) {
                self.state = YTAState::Errored;
            } else {
                debug!("Ignoring non-fatal yt-dlp error during recording: {}", line);
            }
        } else if line.contains("KeyboardInterrupt") || line.contains("User Interrupt") {
            self.state = YTAState::Interrupted;
        } else if line.contains("This live event has ended")
            || line.contains("This is a past livestream")
        {
            self.state = YTAState::AlreadyProcessed;
        } else if line.starts_with("[wait] ") {
            // --wait-for-video countdown
            self.state = YTAState::Waiting(None);
        } else if !line.trim().is_empty()
            && !line.starts_with("[youtube]")
            && !line.starts_with("[info]")
            && !line.starts_with("[debug]")
            && !line.starts_with("WARNING:")
            && !line.starts_with("[download]")
            && !line.starts_with("[wait]")
            && !line.starts_with("frame=")
            && !line.trim_start().starts_with("frame=")
            && !line.contains("[https @ ")
            && !line.contains("[ffmpeg @ ")
            && !line.contains("[hls @ ")
            && !CONCURRENT_PROGRESS_RE.is_match(&line)
        {
            debug!("Unrecognized yt-dlp output: {}", line);
        }
    }
}
