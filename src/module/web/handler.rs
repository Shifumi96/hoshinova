use super::TaskMap;
use crate::{
    config::Config,
    module::{Message, Task},
    msgbus::BusTx,
    youtube,
};
use actix_web::{
    error::{ErrorBadRequest, ErrorInternalServerError},
    get, post, put,
    web::{self, Data},
    HttpResponse, Responder,
};
use anyhow::anyhow;
use serde::Deserialize;
use std::{process::Stdio, sync::Arc};
use tokio::sync::RwLock;
use ts_rs::TS;

#[derive(rust_embed::RustEmbed)]
#[folder = "web/dist"]
struct StaticFiles;

/// Configure routes for the webserver
pub fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.service(get_tasks)
        .service(post_task)
        .service(get_version)
        .service(get_config)
        .service(get_config_toml)
        .service(put_config_toml)
        .service(reload_config)
        .service(serve_static);
}

#[get("/api/tasks")]
async fn get_tasks(data: TaskMap) -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().json(
        data.read()
            .await
            .iter()
            .map(|(_, v)| v.to_owned())
            .collect::<Vec<_>>(),
    ))
}

#[derive(Deserialize, TS)]
#[ts(export, export_to = "web/src/bindings/")]
struct CreateTaskRequest {
    video_url: String,
    output_directory: String,
}

#[post("/api/task")]
async fn post_task(
    tx: Data<BusTx<Message>>,
    config: Data<Arc<RwLock<Config>>>,
    taskreq: web::Json<CreateTaskRequest>,
) -> actix_web::Result<impl Responder> {
    let taskreq = taskreq.into_inner();

    // Make sure the video URL is valid
    let url =
        youtube::URL::parse(&taskreq.video_url).map_err(|e| ErrorBadRequest(format!("{:?}", e)))?;
    let video_id = url
        .video_id()
        .ok_or(ErrorBadRequest(anyhow!("Not a video URL")))?;
    let video_url = format!("https://www.youtube.com/watch?v={}", video_id);

    // Fetch video metadata via yt-dlp --dump-json (avoids fragile HTML scraping)
    info!("Fetching video metadata for {} via yt-dlp", video_id);
    let cfg = config.read().await.ytdlp.clone();
    let mut cmd_args = vec![
        "--dump-json".to_string(),
        "--no-playlist".to_string(),
        "--js-runtimes".to_string(),
        "node".to_string(),
    ];
    if let Some(ref cookies) = cfg.cookies_file {
        let abs_cookies = std::fs::canonicalize(cookies)
            .unwrap_or_else(|_| std::path::PathBuf::from(cookies));
        cmd_args.push("--cookies".to_string());
        cmd_args.push(abs_cookies.to_string_lossy().to_string());
    }
    cmd_args.push(video_url);

    let output = tokio::process::Command::new(&cfg.executable_path)
        .args(&cmd_args)
        .current_dir(&cfg.working_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| ErrorInternalServerError(format!("Failed to run yt-dlp: {:?}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!("yt-dlp --dump-json failed for {}: {}", video_id, stderr);
        return Err(ErrorInternalServerError(format!(
            "yt-dlp failed: {}",
            stderr.lines().last().unwrap_or("unknown error")
        )));
    }

    #[derive(Deserialize)]
    struct YtDlpMeta {
        id: String,
        title: String,
        uploader: String,
        channel_id: String,
        thumbnail: Option<String>,
    }

    let meta: YtDlpMeta = serde_json::from_slice(&output.stdout)
        .map_err(|e| ErrorInternalServerError(format!("Failed to parse yt-dlp output: {:?}", e)))?;

    info!("Got metadata: {} / {}", meta.title, meta.channel_id);

    let task = Task {
        title: meta.title,
        video_id: meta.id,
        video_picture: meta.thumbnail.unwrap_or_default(),
        channel_name: meta.uploader,
        channel_id: meta.channel_id,
        channel_picture: None,
        output_directory: taskreq.output_directory,
    };

    info!("Queuing task for {}", task.video_id);
    tx.send(Message::ToRecord(task))
        .await
        .map_err(|e| {
            error!("Failed to queue task: {:?}", e);
            ErrorInternalServerError(format!("{:?}", e))
        })?;

    Ok(HttpResponse::Accepted().finish())
}

#[get("/api/version")]
async fn get_version() -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().body(crate::APP_NAME.to_owned()))
}

#[get("/api/config")]
async fn get_config(config: Data<Arc<RwLock<Config>>>) -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().json(config.read().await.to_owned()))
}

#[post("/api/config/reload")]
async fn reload_config(config: Data<Arc<RwLock<Config>>>) -> actix_web::Result<impl Responder> {
    config
        .write()
        .await
        .reload()
        .await
        .map_err(|e| ErrorInternalServerError(format!("{:?}", e)))?;
    Ok(HttpResponse::Ok().json("ok"))
}

#[get("/api/config/toml")]
async fn get_config_toml(config: Data<Arc<RwLock<Config>>>) -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().body(
        config
            .read()
            .await
            .get_source_toml()
            .await
            .map_err(|e| ErrorInternalServerError(format!("{:?}", e)))?,
    ))
}

#[put("/api/config/toml")]
async fn put_config_toml(
    config: Data<Arc<RwLock<Config>>>,
    body: web::Bytes,
) -> actix_web::Result<impl Responder> {
    let body = std::str::from_utf8(&body).map_err(|e| ErrorBadRequest(format!("{:?}", e)))?;
    config
        .write()
        .await
        .set_source_toml(body)
        .await
        .map_err(|e| ErrorBadRequest(format!("{:?}", e)))?;
    Ok(HttpResponse::Ok().json("ok"))
}

#[get("/{_:.*}")]
async fn serve_static(path: web::Path<String>) -> impl Responder {
    let mut path = path.into_inner();
    if path.is_empty() {
        path = "index.html".to_string();
    }

    // If debug mode, serve the files from the static folder
    #[cfg(debug_assertions)]
    return tokio::fs::read(format!("web/dist/{}", path))
        .await
        .map(|bytes| {
            HttpResponse::Ok()
                .content_type(mime_guess::from_path(path).first_or_octet_stream().as_ref())
                .body(bytes)
        })
        .unwrap_or_else(|_| HttpResponse::NotFound().body("404"));

    // Otherwise serve the files from the embedded folder
    #[cfg(not(debug_assertions))]
    return match StaticFiles::get(&path) {
        Some(content) => HttpResponse::Ok()
            .content_type(mime_guess::from_path(path).first_or_octet_stream().as_ref())
            .body(content.data.into_owned()),
        None => HttpResponse::NotFound().body("404 Not Found"),
    };
}
