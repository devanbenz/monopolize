use std::ffi::OsStr;
use std::path::Path;

#[derive(Clone, Debug)]
pub enum FileType {
    Parquet,
    Csv,
    Json,
    Orc,
}

pub fn get_file_type(path: &Path) -> FileType {
    let ext = path.extension().and_then(OsStr::to_str);
    match ext {
        Some("parquet") => FileType::Parquet,
        Some("csv") => FileType::Csv,
        Some("json") => FileType::Json,
        Some("orc") => FileType::Orc,
        None => unimplemented!("file type not supported or there is no extension"),
        _ => panic!("some how the program ended up here, this should not be possible :("),
    }
}
