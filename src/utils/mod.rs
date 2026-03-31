use std::path::Path;

pub fn same_path(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => from == to,
        _ => false,
    }
}

pub fn file_to_file(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => {
            let from_is_file = Path::new(from).is_file();
            from_is_file && valid_parent_dir(Some(to)) && !same_path(Some(from), Some(to))
        },
        _ => false,
    }
}

pub fn valid_mv(from_path: Option<&str>, to_path: Option<&str>, preserve_extension: bool) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => {
            if file_to_dir(Some(from), Some(to)) {
                true
            } else if file_to_file(Some(from), Some(to)) {
                if preserve_extension && !same_extension(from_path, to_path) {
                    return false;
                }
                true
            } else {
                let from_is_dir = Path::new(from).is_dir();
                let to_is_not_file = !Path::new(to).is_file();
                from_is_dir && to_is_not_file && valid_dir_path(Some(to))
            }
        },
        _ => false,
    }
}

pub fn file_to_dir(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) => {
            let from_is_file = Path::new(from).is_file();
            let to_is_dir = Path::new(to).is_dir();
            from_is_file && to_is_dir
        },
        _ => false,
    }
}

pub fn valid_dir_path(path_str: Option<&str>) -> bool {
    match path_str {
        Some(s) if !s.is_empty() => {
            let path = Path::new(s);

            if path.exists() {
                return path.is_dir();
            }

            match path.parent() {
                Some(parent) if parent.as_os_str().is_empty() => Path::new(".").is_dir(),
                Some(parent) => parent.is_dir(),
                None => false,
            }
        },
        _ => false,
    }
}

pub fn valid_parent_dir(path_str: Option<&str>) -> bool {
    match path_str {
        Some(s) if !s.is_empty() => {
            let path = Path::new(s);
            match path.parent() {
                Some(parent) if parent.as_os_str().is_empty() => Path::new(".").is_dir(),
                Some(parent) => parent.is_dir(),
                None => false,
            }
        },
        _ => false,
    }
}

pub fn same_extension(from_path: Option<&str>, to_path: Option<&str>) -> bool {
    match (from_path, to_path) {
        (Some(from), Some(to)) if !to.is_empty() => {
            let from_path = Path::new(from);
            let to_path = Path::new(to);

            if let (Some(from_ext), Some(to_ext)) = (from_path.extension(), to_path.extension()) {
                from_ext == to_ext
            } else {
                false
            }
        },
        _ => false,
    }
}
