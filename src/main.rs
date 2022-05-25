#![allow(dead_code, unused_variables)]

use std::io::{Read, stdin};
use std::path::PathBuf;

use clap::{Parser, Subcommand, ArgGroup};
use chrono::{Local, DateTime};
use serde::Deserialize;

mod db;
use db::{Db, FsDb};

#[derive(Parser)]
#[clap(group(
    ArgGroup::new("conf")
        .multiple(false)
        .args(&["config", "dir"])
    ))]
struct App {
    #[clap(short, long)]
    config : Option<String>,
    #[clap(short, long)]
    dir : Option<String>,
    #[clap(subcommand)]
    command : Option<Commands>,
}


#[derive(Subcommand)]
enum Commands {
    List{
        issue : Option<u32>,
    },
    Issue,
    Comment {
        issue_id : u32,
    },
}

impl Default for Commands {
    fn default() -> Self {
        Self::List{ issue : None }
    }
}



fn default_db() -> String {
    ".praiadb".to_string()
}

#[derive(Default, Deserialize)]
struct Config {
    #[serde(skip)]
    path : PathBuf,

    upstream : Option<String>,

    #[serde(default = "default_db")]
    db : String,
}

fn get_project_dir(config_flag : Option<String>) -> Option<PathBuf> {
    if let Some(dir) = config_flag {
        return Some(dir.into())
    }

    if let Ok(dir) = std::env::var("PRAIA_CONF") {
        return Some(dir.into())
    }

    let mut cwd : PathBuf = match std::env::current_dir() {
        Err(err) => {
            eprintln!("{err}");
            return None
        },
        Ok(s) => s.into(),
    };


    loop {
        cwd.push("praia.toml");

        if cwd.exists() {
            return Some(cwd)
        }

        cwd.pop();
        if !cwd.pop() {
            return None
        }
    }
}


fn main() {
    let app = App::parse();

    let config = if let Some(dir) = app.dir {
        Config{
            path : "".into(),
            db : dir,
            ..Default::default()
        }
    } else {
        let mut config_path : PathBuf = if let Some(config_path) = app.config {
            config_path.into()
        } else {
            get_project_dir(app.config)
                .expect("praia config not found")
        };

        let config_str = std::fs::read_to_string(&config_path)
            .expect("error reading config");

        let mut config : Config = toml::from_str(&config_str)
            .expect("error parsing config");

        config_path.pop();
        config.path = config_path;

        config
    };

    let mut db_path = config.path.clone();
    db_path.push(config.db);

    let db = FsDb::new(db_path).unwrap();

    match app.command.unwrap_or_default() {
        Commands::Issue => {
            let mut buf = String::new();
            stdin().read_to_string(&mut buf).unwrap();

            if buf.trim().is_empty() {
                eprintln!("input empty, exiting");
                return
            }

            let id = db.new_issue(buf.as_str()).unwrap();
            db.save_index().unwrap();
            println!("{id}");
        },
        Commands::Comment{issue_id} => {
            let issue = db.get_issue(issue_id).unwrap();
            println!(
                "/{}\t{}",
                issue.issue_id,
                issue.content.trim_end()
            );

            let mut buf = String::new();
            stdin().read_to_string(&mut buf).unwrap();

            if buf.trim().is_empty() {
                eprintln!("input empty, exiting");
                return
            }

            let id = db.new_comment(issue_id , buf.as_str()).unwrap();
            db.save_index().unwrap();
            println!("{id}");
        },
        Commands::List{issue} => {
            if let Some(issue) = issue {
                let it = db.get_issue_comments(issue);
                for comment_res in it {
                    let comment = comment_res.unwrap();
                    print!(
                        "/{}/{}\t{}\n\n",
                        comment.issue_id,
                        comment.comment_id,
                        DateTime::<Local>::from(comment.created).to_rfc2822()
                    );

                    for line in comment.content.trim_end().lines() {
                        println!("\t{line}");
                    }

                    println!("");
                }
            } else {
                let it = db.get_issues();
                for issue_res in it {
                    let issue = issue_res.unwrap();
                    println!(
                        "/{}\t{}",
                        issue.issue_id,
                        issue.content.trim_end()
                    );
                }
            }
        },
    }
}


