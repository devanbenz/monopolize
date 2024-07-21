use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("There was an error reading {0} from the file system. Please make sure you are using an existing file for merging.")]
    FileNotFoundError(String)
}
