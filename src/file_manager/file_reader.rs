pub struct FileReader(String);

impl FileReader {
    pub fn new(file_name: String) -> Self {
        Self(file_name)
    }
}
