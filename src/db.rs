use std::result::Result;
use std::io::{Read, Write, BufRead, BufReader, ErrorKind};
use std::fs::{File, OpenOptions, read_dir, create_dir};
use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::collections::HashMap;

use quick_from::QuickFrom;

#[derive(Debug, Clone)]
pub struct Comment {
    pub issue_id : u32,
    pub comment_id : u32,
    pub created : SystemTime,
    pub modified : SystemTime,
    pub content : String,
}

#[derive(Debug, Clone)]
pub struct Issue {
    pub issue_id : u32,
    pub created : SystemTime,
    pub modified : SystemTime,
    /// the first comment
    pub content : String,
}

pub type BoxIter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub trait Db {
    type Error : std::error::Error;

    fn new_issue(&self, first_comment : &str) -> Result<u32, Self::Error>;
    fn new_comment(&self, issue_id : u32, content : &str) -> Result<u32, Self::Error>;

    fn get_issues<'a>(&'a self) -> BoxIter<'a, Result<Issue, Self::Error>>;
    fn get_issue(&self, issue_id : u32) -> Result<Issue, Self::Error>;
    fn get_issue_comments(&self, issue_id : u32) -> BoxIter<Result<Comment, Self::Error>>;
    fn get_issue_comment(&self, issue_id : u32, comment_id : u32) -> Result<Comment, Self::Error>;
}


#[derive(Debug, QuickFrom)]
pub enum FsError {
    BadIndex,
    BadDb,

    NoIssue(u32),

    #[quick_from]
    Io(std::io::Error),
}

impl std::error::Error for FsError {}

impl std::fmt::Display for FsError {
    fn fmt(&self, f : &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use FsError::*;

        match self {
            BadIndex => write!(f, "index corrupted"),
            BadDb => write!(f, "db corrupted"),
            NoIssue(id) => write!(f, "issue {id} not found"),
            Io(err) => std::fmt::Display::fmt(err, f),
        }
    }
}

// TODO: implement file lock
pub struct FsDb(RwLock<FsDbInner>);

struct FsDbInner {
    path : Box<Path>,
    issue_count : u32,
    comment_count : HashMap<u32, u32>,
}

impl FsDbInner {
    fn create_index(path : Box<Path>) -> Result<Self, FsError> {
        let index_path = path.join("index.txt");

        let mut issue_count = 0;
        let mut comment_count = HashMap::new();

        for issue_res in read_dir(&path)? {
            let issue = issue_res?;

            let name = issue.file_name();
            if name == "index.txt" {
                continue
            }

            let issue_id = name
                .to_str().ok_or(FsError::BadDb)?
                .parse().map_err(|_| FsError::BadDb)?;

            issue_count = issue_count.max(issue_id+1);

            let mut max_comment = 0;

            for comment_res in read_dir(issue.path())? {
                let comment = comment_res?;

                let comment_id : u32 = comment.file_name()
                    .to_str().ok_or(FsError::BadDb)?
                    .parse().map_err(|_| FsError::BadDb)?;

                max_comment = max_comment.max(comment_id+1);
            }

            comment_count.insert(issue_id, max_comment);
        }

        let ret = Self{path, issue_count, comment_count};

        ret.save_index()?;

        Ok(ret)
    }

    fn read_index(path : Box<Path>) -> Result<Self, FsError> {

        let index_path = path.join("index.txt");

        let mut index = BufReader::new(File::open(&index_path)?);

        let mut buf = String::new();

        let issue_count = if index.read_line(&mut buf)? > 0 {
            buf.trim_end().parse().map_err(|_| FsError::BadIndex)?
        } else {
            return Err(FsError::BadIndex);
        };

        buf.clear();

        let mut comment_count = HashMap::new();

        while index.read_line(&mut buf)? > 0 {

            let mut it = buf.split(" ");

            let k : u32 = it.next()
                .ok_or(FsError::BadIndex)?
                .parse()
                .map_err(|_| FsError::BadIndex)?;

            let v : u32 = it.next()
                .ok_or(FsError::BadIndex)?
                .trim_end()
                .parse()
                .map_err(|_| FsError::BadIndex)?;

            if it.next().is_some() {
                return Err(FsError::BadIndex);
            }

            comment_count.insert(k, v);

            buf.clear();
        }

        Ok(Self{path, issue_count, comment_count})
    }

    fn save_index(&self) -> Result<(), FsError> {

        let index_path = self.path.join("index.txt");

        let mut index = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(index_path)?;

        writeln!(index, "{}", self.issue_count)?;

        for (k, v) in self.comment_count.iter() {
            writeln!(index, "{k} {v}")?;
        }

        Ok(())
    }
}

impl FsDb {
    pub fn new(path : PathBuf) -> Result<Self, FsError> {
        let path = path.into_boxed_path();
        let inner = if path.join("index.txt").exists() {
            FsDbInner::read_index(path)?
        } else {
            FsDbInner::create_index(path)?
        };

        Ok(Self(RwLock::new(inner)))
    }

    pub fn save_index(&self) -> Result<(), FsError> {
        self.0.write().unwrap().save_index()
    }


}



impl Db for FsDb {
    type Error = FsError;

    fn new_issue(&self, first_comment : &str) -> Result<u32, Self::Error> {
        let mut db = self.0.write().unwrap();
        let issue_id = db.issue_count;

        let mut path = db.path.join(issue_id.to_string());

        create_dir(&path)?;

        db.comment_count.insert(issue_id, 1);
        db.issue_count += 1;

        path.push("0");

        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.write_all(first_comment.as_bytes())?;

        Ok(issue_id)
    }


    fn new_comment(&self, issue_id : u32, content : &str) -> Result<u32, Self::Error> {
        let mut db = self.0.write().unwrap();

        let mut path = db.path.join(issue_id.to_string());

        let ent = db.comment_count.get_mut(&issue_id).ok_or(FsError::NoIssue(issue_id))?;
        let comment_id = *ent;

        path.push(comment_id.to_string());

        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.write_all(content.as_bytes())?;

        *ent = *ent + 1;

        Ok(comment_id)
    }

    fn get_issues<'a>(&'a self) -> BoxIter<'a, Result<Issue, Self::Error>> {
        let db = self.0.read().unwrap();
        let mut path = db.path.to_path_buf();

        let it = (0..db.issue_count).filter_map(move |issue_id| {
            // bring in the lock
            let _db = &db;

            path.push(issue_id.to_string());
            path.push("0");

            let file_res = File::open(&path);

            path.pop();
            path.pop();


            let file = match file_res {
                Ok(file) => file,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    return None
                },
                Err(err) => return Some(Err(err.into())),
            };

            let go = || {

                let meta = file.metadata()?;

                let mut buf = String::new();
                BufReader::new(file).read_line(&mut buf)?;

                Result::<_, FsError>::Ok(Issue{
                    issue_id,
                    created : meta.created()?,
                    modified : meta.modified()?,
                    content : buf,
                })
            };

            Some(go())
        });

        Box::new(it)
    }

    fn get_issue(&self, issue_id : u32) -> Result<Issue, Self::Error> {
        let db = self.0.read().unwrap();

        let mut path = db.path.join(issue_id.to_string());
        path.push("0");

        let mut file = File::open(path)?;
        let meta = file.metadata()?;

        let mut buf = String::new();
        file.read_to_string(&mut buf)?;

        Ok(Issue{
            issue_id,
            created : meta.created()?,
            modified : meta.modified()?,
            content : buf,
        })
    }

    fn get_issue_comments(&self, issue_id : u32) -> BoxIter<Result<Comment, Self::Error>> {
        let db = self.0.read().unwrap();

        let mut path = db.path.join(issue_id.to_string());
        let count = match db.comment_count.get(&issue_id) {
            None => {
                let res = Err(FsError::NoIssue(issue_id));
                return Box::new(std::iter::once(res))
            },
            Some(n) => *n,
        };

        let it = (0..count).filter_map(move |comment_id| {
            // bring in the lock
            let _db = &db;

            path.push(comment_id.to_string());
            let file_res = File::open(&path);
            path.pop();


            let mut file = match file_res {
                Ok(file) => file,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    return None
                },
                Err(err) => return Some(Err(err.into())),
            };

            let mut go = || {
                let meta = file.metadata()?;

                let mut buf = String::new();
                file.read_to_string(&mut buf)?;

                Ok(Comment{
                    issue_id,
                    comment_id,
                    created : meta.created()?,
                    modified : meta.modified()?,
                    content : buf,
                })
            };

            Some(go())
        });

        Box::new(it)
    }

    fn get_issue_comment(&self, issue_id : u32, comment_id : u32) -> Result<Comment, Self::Error> {
        let db = self.0.read().unwrap();

        let mut path = db.path.join(issue_id.to_string());
        path.push(comment_id.to_string());

        let mut file = File::open(path)?;
        let meta = file.metadata()?;

        let mut buf = String::new();
        file.read_to_string(&mut buf)?;

        Ok(Comment{
            issue_id,
            comment_id,
            created : meta.created()?,
            modified : meta.modified()?,
            content : buf,
        })
    }
}
