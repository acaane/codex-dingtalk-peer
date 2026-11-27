#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod agent_binding;
mod agent_start;
mod agent_view;
mod chat;
mod codex;
mod file_view;
mod readiness;
mod room;
mod room_files;
mod room_sessions;
mod room_view;
mod storage;
mod task_view;
mod tasks;
mod ui;

use std::{error::Error, process::ExitCode};

use futures_lite::future::block_on;
use gpui_kit::*;

use crate::chat::{ChatStore, Message};

async fn open_chat() -> Result<(ChatStore, Vec<Message>), Box<dyn Error>> {
    let directory = storage::data_directory()?;
    let mut store = ChatStore::open(&directory.join("chat.sqlite3")).await?;
    let history = store.history().await?;
    Ok((store, history))
}

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if !args.is_empty() {
        return match agent_start::command(&args) {
            Ok(receipt) => {
                println!("{receipt}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        };
    }
    application().with_assets(assets::Assets).run(|cx| {
        init(cx);
        let options = WindowOptions {
            titlebar: Some(TitlebarOptions {
                title: Some("AI 联调台".into()),
                ..Default::default()
            }),
            window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                None,
                size(px(900.), px(680.)),
                cx,
            ))),
            window_min_size: Some(size(px(440.), px(340.))),
            ..Default::default()
        };
        let chat = block_on(open_chat());
        open_window(options, cx, |window, cx| {
            cx.new(|cx| ui::ChatWindow::new(chat, window, cx))
        })
        .expect("打开 AI 联调台窗口");
    });
    ExitCode::SUCCESS
}
