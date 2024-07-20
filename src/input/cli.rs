use std::path::Path;
use clap::Parser;
use clap_derive::Parser as DParser;
use crate::errors::error::Error;

#[derive(DParser, Debug)]
#[command(version, about, long_about = None)]
pub struct FileMergeArgs {
    // Files to merge
    #[arg(short, long, value_delimiter = ' ', num_args = 1..)]
    pub files: Vec<String>,
}

pub fn handle_input() -> Result<Vec<String>, Error> {
    let mut files_to_merge = vec![];
    let file_args = FileMergeArgs::parse();

    for file in file_args.files {
        if read_from_fs(&file) == false {
            return Err(Error::FileNotFoundError(file));
        } else {
            files_to_merge.push(file);
        }
    }

    Ok(files_to_merge)
}

fn read_from_fs(file: &str) -> bool {
    Path::new(file).exists()
}
