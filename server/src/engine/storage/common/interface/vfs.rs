/*
 * Created on Thu Feb 29 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use {
    crate::{engine::sync::cell::Lazy, util::test_utils, IoResult},
    parking_lot::RwLock,
    std::{
        collections::{
            hash_map::{Entry, OccupiedEntry},
            HashMap,
        },
        io::{Error, ErrorKind},
    },
};

/*
    virtual fs impl
    ---
*/

pub mod vfs_utils {
    #[derive(Debug, PartialEq, Clone, Copy)]
    pub(super) enum WriteCrashKind {
        None,
        Zero,
        Random,
    }
    local!(
        static RANDOM_WRITE_CRASH: WriteCrashKind = WriteCrashKind::None;
        pub(super) static RNG: Option<rand::rngs::ThreadRng> = None;
    );
    /// WARNING: A random write crash automatically degrades to a [`WriteCrashKind::Zero`] as soon as it completes
    /// to prevent any further data writes (due to retries in
    /// [`fs::FileWrite::fwrite_all_count`](super::super::fs::FileWrite::fwrite_all_count))
    pub fn debug_enable_random_write_crash() {
        local_mut!(RANDOM_WRITE_CRASH, |crash| *crash = WriteCrashKind::Random)
    }
    pub fn debug_enable_zero_write_crash() {
        local_mut!(RANDOM_WRITE_CRASH, |crash| *crash = WriteCrashKind::Zero)
    }
    pub fn debug_disable_write_crash() {
        local_mut!(RANDOM_WRITE_CRASH, |crash| *crash = WriteCrashKind::None)
    }
    pub(super) fn debug_write_crash_setting() -> WriteCrashKind {
        local_ref!(RANDOM_WRITE_CRASH, |crash| *crash)
    }
}

/*
    definitions
    ---
    fs, node, dir, file
*/

/// A virtual directory
type VDir = HashMap<Box<str>, VNode>;
/// An iterator over the components of a file path (alias)
type ComponentIter<'a> = std::iter::Take<std::vec::IntoIter<&'a str>>;

pub struct VirtualFS {
    root: HashMap<Box<str>, VNode>,
}

#[derive(Debug)]
enum VNode {
    Dir(HashMap<Box<str>, Self>),
    File(RwLock<VFile>),
}

impl VNode {
    fn clone_into_new_node(&self) -> Self {
        match self {
            Self::Dir(d) => Self::Dir(
                d.iter()
                    .map(|(id, data)| (id.clone(), data.clone_into_new_node()))
                    .collect(),
            ),
            Self::File(f) => Self::File(RwLock::new(f.read().clone_to_new_file())),
        }
    }
}

#[derive(Debug)]
pub(super) struct VFile {
    read: bool,
    write: bool,
    data: Vec<u8>,
    pos: usize,
}

#[derive(Debug, PartialEq)]
/// Result of opening a file
/// - Created: newly created file
/// - Existing: existing file that was reopened
pub enum FileOpen<CF, EF = CF> {
    /// new file
    Created(CF),
    /// existing file
    Existing(EF),
}

#[derive(Debug)]
pub struct VFileDescriptor(pub(super) Box<str>);

impl Drop for VFileDescriptor {
    fn drop(&mut self) {
        VirtualFS::instance()
            .write()
            .with_file_mut(&self.0, |f| {
                f.pos = 0;
                f.write = false;
                f.read = false;
                Ok(())
            })
            .unwrap()
    }
}

/*
    impl
*/

impl VFile {
    pub fn clone_to_new_file(&self) -> Self {
        Self {
            read: false,
            write: false,
            data: self.data.clone(),
            pos: 0,
        }
    }
    pub fn truncate(&mut self, to: u64) -> IoResult<()> {
        if !self.write {
            return Err(Error::new(ErrorKind::PermissionDenied, "Write permission denied").into());
        }
        if to as usize > self.data.len() {
            self.data.resize(to as usize, 0);
        } else {
            self.data.truncate(to as usize);
        }
        if self.pos > self.data.len() {
            self.pos = self.data.len();
        }
        Ok(())
    }
    pub fn length(&self) -> IoResult<u64> {
        Ok(self.data.len() as u64)
    }
    pub fn cursor(&self) -> IoResult<u64> {
        Ok(self.pos as u64)
    }
    pub fn seek_from_start(&mut self, by: u64) -> IoResult<()> {
        if by > self.data.len() as u64 {
            return Err(Error::new(ErrorKind::InvalidInput, "Can't seek beyond file's end").into());
        }
        self.pos = by as usize;
        Ok(())
    }
    pub fn fread_exact(&mut self, buf: &mut [u8]) -> IoResult<()> {
        if !self.read {
            return Err(Error::new(ErrorKind::PermissionDenied, "Read permission denied").into());
        }
        let available_bytes = self.current().len();
        if available_bytes < buf.len() {
            return Err(Error::from(ErrorKind::UnexpectedEof).into());
        }
        buf.copy_from_slice(&self.data[self.pos..self.pos + buf.len()]);
        self.pos += buf.len();
        Ok(())
    }
    pub fn fwrite(&mut self, bytes: &[u8]) -> IoResult<u64> {
        if !self.write {
            return Err(Error::new(ErrorKind::PermissionDenied, "Write permission denied").into());
        }
        match vfs_utils::debug_write_crash_setting() {
            vfs_utils::WriteCrashKind::None => {
                if self.pos + bytes.len() > self.data.len() {
                    self.data.resize(self.pos + bytes.len(), 0);
                }
                self.data[self.pos..self.pos + bytes.len()].copy_from_slice(bytes);
                self.pos += bytes.len();
                Ok(bytes.len() as _)
            }
            vfs_utils::WriteCrashKind::Random => {
                let actual_write_length = local_mut!(vfs_utils::RNG, |rng| {
                    match rng {
                        Some(ref mut rng) => test_utils::random_number(0, bytes.len(), rng),
                        None => {
                            let mut rng_ = rand::thread_rng();
                            let r = test_utils::random_number(0, bytes.len(), &mut rng_);
                            *rng = Some(rng_);
                            r
                        }
                    }
                });
                // write some random part of the buffer into this file
                if self.pos + actual_write_length > self.data.len() {
                    self.data.resize(self.pos + actual_write_length, 0);
                }
                self.data[self.pos..self.pos + actual_write_length]
                    .copy_from_slice(&bytes[..actual_write_length]);
                self.pos += actual_write_length;
                // now soon as this is complete, downgrade error type to writezero so that we don't write any further data during
                // a retry
                vfs_utils::debug_enable_zero_write_crash();
                Ok(actual_write_length as _)
            }
            vfs_utils::WriteCrashKind::Zero => Ok(0),
        }
    }
}

impl VFile {
    fn new(read: bool, write: bool, data: Vec<u8>, pos: usize) -> Self {
        Self {
            read,
            write,
            data,
            pos,
        }
    }
    fn current(&self) -> &[u8] {
        &self.data[self.pos..]
    }
}

impl VNode {
    fn as_dir_mut(&mut self) -> Option<&mut VDir> {
        match self {
            Self::Dir(d) => Some(d),
            Self::File(_) => None,
        }
    }
}

impl VirtualFS {
    pub fn instance() -> &'static RwLock<Self> {
        static GLOBAL_VFS: Lazy<RwLock<VirtualFS>, fn() -> RwLock<VirtualFS>> =
            Lazy::new(|| RwLock::new(VirtualFS::new()));
        &GLOBAL_VFS
    }
    pub fn get_data(&self, path: &str) -> IoResult<Vec<u8>> {
        self.with_file(path, |f| Ok(f.data.clone()))
    }
    pub fn fs_copy(&mut self, from: &str, to: &str) -> IoResult<()> {
        let node = self.with_item(from, |node| Ok(node.clone_into_new_node()))?;
        // process components
        let (target, components) = util::split_target_and_components(to);
        let mut current = &mut self.root;
        for component in components {
            match current.get_mut(component) {
                Some(VNode::Dir(dir)) => {
                    current = dir;
                }
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => return err::dir_missing_in_path(),
            }
        }
        match current.entry(target.into()) {
            Entry::Occupied(mut item) => {
                item.insert(node);
            }
            Entry::Vacant(ve) => {
                ve.insert(node);
            }
        }
        Ok(())
    }
    pub fn fs_fcreate_rw(&mut self, fpath: &str) -> IoResult<VFileDescriptor> {
        let (target_file, components) = util::split_target_and_components(fpath);
        let target_dir = util::find_target_dir_mut(components, &mut self.root)?;
        match target_dir.entry(target_file.into()) {
            Entry::Occupied(k) => {
                match k.get() {
                    VNode::Dir(_) => {
                        return Err(Error::new(
                            ErrorKind::AlreadyExists,
                            "found directory with same name where file was to be created",
                        )
                        .into());
                    }
                    VNode::File(_) => {
                        // the file already exists
                        return Err(Error::new(
                            ErrorKind::AlreadyExists,
                            "the file already exists",
                        )
                        .into());
                    }
                }
            }
            Entry::Vacant(v) => {
                // no file exists, we can create this
                v.insert(VNode::File(RwLock::new(VFile::new(true, true, vec![], 0))));
                Ok(VFileDescriptor(fpath.into()))
            }
        }
    }
    pub fn fs_fopen_rw(
        &mut self,
        fpath: &str,
        read: bool,
        write: bool,
    ) -> IoResult<VFileDescriptor> {
        self.with_file_mut(fpath, |f| {
            f.read = read;
            f.write = write;
            Ok(VFileDescriptor(fpath.into()))
        })
    }
    pub fn fs_rename(&mut self, from: &str, to: &str) -> IoResult<()> {
        // get file data
        let data = self.with_file(from, |f| Ok(f.data.clone()))?;
        // get root dir
        let mut current = &mut self.root;
        // process components
        let (target, mut components) = util::split_target_and_components(to);
        while let Some(component) = components.next() {
            match current.get_mut(component) {
                Some(VNode::Dir(d)) => {
                    current = d;
                }
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => return err::dir_missing_in_path(),
            }
        }
        let _ = current.insert(
            target.into(),
            VNode::File(RwLock::new(VFile {
                read: false,
                write: false,
                data,
                pos: 0,
            })),
        );
        self.fs_remove_file(from)
    }
    pub fn fs_remove_file(&mut self, fpath: &str) -> IoResult<()> {
        self.with_item_mut(fpath, |e| match e.get() {
            VNode::File(_) => {
                e.remove();
                Ok(())
            }
            _ => return err::item_is_not_file(),
        })
    }
    pub fn fs_create_dir(&mut self, fpath: &str) -> IoResult<()> {
        // get root dir
        let mut current = &mut self.root;
        // process components
        let (target, mut components) = util::split_target_and_components(fpath);
        while let Some(component) = components.next() {
            match current.get_mut(component) {
                Some(VNode::Dir(d)) => {
                    current = d;
                }
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => return err::dir_missing_in_path(),
            }
        }
        match current.entry(target.into()) {
            Entry::Occupied(_) => return Err(Error::from(ErrorKind::AlreadyExists).into()),
            Entry::Vacant(ve) => {
                ve.insert(VNode::Dir(into_dict!()));
                Ok(())
            }
        }
    }
    pub fn fs_create_dir_all(&mut self, fpath: &str) -> IoResult<()> {
        fn create_ahead(mut ahead: &[&str], current: &mut VDir) -> IoResult<()> {
            if ahead.is_empty() {
                return Ok(());
            }
            let this = ahead[0];
            ahead = &ahead[1..];
            match current.get_mut(this) {
                Some(VNode::Dir(d)) => {
                    if ahead.is_empty() {
                        // hmm, this was the list dir that was to be created, but it already exists
                        return Err(Error::from(ErrorKind::AlreadyExists).into());
                    }
                    return create_ahead(ahead, d);
                }
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => {
                    let _ = current.insert(this.into(), VNode::Dir(into_dict!()));
                    let dir = current.get_mut(this).unwrap().as_dir_mut().unwrap();
                    return create_ahead(ahead, dir);
                }
            }
        }
        let pieces = util::split_parts(fpath);
        create_ahead(&pieces, &mut self.root)
    }
    pub fn fs_delete_dir(&mut self, fpath: &str) -> IoResult<()> {
        self.dir_delete(fpath, false)
    }
    pub fn fs_delete_dir_all(&mut self, fpath: &str) -> IoResult<()> {
        self.dir_delete(fpath, true)
    }
}

impl VirtualFS {
    fn new() -> Self {
        Self {
            root: HashMap::new(),
        }
    }
    #[allow(unused)]
    fn fs_fopen_or_create_rw(&mut self, fpath: &str) -> IoResult<FileOpen<VFileDescriptor>> {
        // components
        let (target_file, components) = util::split_target_and_components(fpath);
        let target_dir = util::find_target_dir_mut(components, &mut self.root)?;
        match target_dir.entry(target_file.into()) {
            Entry::Occupied(oe) => match oe.get() {
                VNode::File(f) => {
                    let mut f = f.write();
                    f.read = true;
                    f.write = true;
                    Ok(FileOpen::Existing(VFileDescriptor(fpath.into())))
                }
                VNode::Dir(_) => return err::item_is_not_file(),
            },
            Entry::Vacant(v) => {
                v.insert(VNode::File(RwLock::new(VFile::new(true, true, vec![], 0))));
                Ok(FileOpen::Created(VFileDescriptor(fpath.into())))
            }
        }
    }
    pub(super) fn with_file_mut<T>(
        &self,
        fpath: &str,
        f: impl FnOnce(&mut VFile) -> IoResult<T>,
    ) -> IoResult<T> {
        let (target_file, components) = util::split_target_and_components(fpath);
        let target_dir = util::find_target_dir(components, &self.root)?;
        match target_dir.get(target_file) {
            Some(VNode::File(file)) => {
                let mut file = file.write();
                f(&mut file)
            }
            Some(VNode::Dir(_)) => return err::item_is_not_file(),
            None => return Err(Error::from(ErrorKind::NotFound).into()),
        }
    }
    pub(super) fn with_file<T>(
        &self,
        fpath: &str,
        f: impl FnOnce(&VFile) -> IoResult<T>,
    ) -> IoResult<T> {
        self.with_item(fpath, |node| match node {
            VNode::File(file) => {
                let f_ = file.read();
                f(&f_)
            }
            VNode::Dir(_) => err::item_is_not_file(),
        })
    }
    fn with_item_mut<T>(
        &mut self,
        fpath: &str,
        f: impl Fn(OccupiedEntry<Box<str>, VNode>) -> IoResult<T>,
    ) -> IoResult<T> {
        // process components
        let (target, components) = util::split_target_and_components(fpath);
        let mut current = &mut self.root;
        for component in components {
            match current.get_mut(component) {
                Some(VNode::Dir(dir)) => {
                    current = dir;
                }
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => return err::dir_missing_in_path(),
            }
        }
        match current.entry(target.into()) {
            Entry::Occupied(item) => return f(item),
            Entry::Vacant(_) => return err::could_not_find_item(),
        }
    }
    fn with_item<T>(&self, fpath: &str, f: impl FnOnce(&VNode) -> IoResult<T>) -> IoResult<T> {
        // process components
        let (target, components) = util::split_target_and_components(fpath);
        let mut current = &self.root;
        for component in components {
            match current.get(component) {
                Some(VNode::Dir(dir)) => {
                    current = dir;
                }
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => return err::dir_missing_in_path(),
            }
        }
        match current.get(target.into()) {
            Some(item) => return f(item),
            None => return err::could_not_find_item(),
        }
    }
    fn dir_delete(&mut self, fpath: &str, allow_if_non_empty: bool) -> IoResult<()> {
        self.with_item_mut(fpath, |node| match node.get() {
            VNode::Dir(d) => {
                if allow_if_non_empty || d.is_empty() {
                    node.remove();
                    return Ok(());
                }
                return Err(Error::new(ErrorKind::InvalidInput, "directory is not empty").into());
            }
            VNode::File(_) => return err::file_in_dir_path(),
        })
    }
}

mod err {
    //! Errors
    //!
    //! These are custom errors returned by the virtual file system
    use {
        crate::IoResult,
        std::io::{Error, ErrorKind},
    };
    pub(super) fn item_is_not_file<T>() -> IoResult<T> {
        Err(Error::new(ErrorKind::InvalidInput, "found directory, not a file").into())
    }
    pub(super) fn file_in_dir_path<T>() -> IoResult<T> {
        Err(Error::new(ErrorKind::InvalidInput, "found file in directory path").into())
    }
    pub(super) fn dir_missing_in_path<T>() -> IoResult<T> {
        Err(Error::new(ErrorKind::InvalidInput, "could not find directory in path").into())
    }
    pub(super) fn could_not_find_item<T>() -> IoResult<T> {
        Err(Error::new(ErrorKind::NotFound, "could not find item").into())
    }
}

mod util {
    use {
        super::{err, ComponentIter, VDir, VNode},
        crate::IoResult,
    };
    pub(super) fn split_parts(fpath: &str) -> Vec<&str> {
        fpath.split("/").collect()
    }
    pub(super) fn split_target_and_components(fpath: &str) -> (&str, ComponentIter) {
        let parts = split_parts(fpath);
        let target = parts.last().unwrap();
        let component_len = parts.len() - 1;
        (target, parts.into_iter().take(component_len))
    }
    pub(super) fn find_target_dir_mut<'a>(
        components: ComponentIter,
        mut current: &'a mut VDir,
    ) -> IoResult<&'a mut VDir> {
        for component in components {
            match current.get_mut(component) {
                Some(VNode::Dir(d)) => current = d,
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => return err::dir_missing_in_path(),
            }
        }
        Ok(current)
    }
    pub(super) fn find_target_dir<'a>(
        components: ComponentIter,
        mut current: &'a VDir,
    ) -> IoResult<&'a VDir> {
        for component in components {
            match current.get(component) {
                Some(VNode::Dir(d)) => current = d,
                Some(VNode::File(_)) => return err::file_in_dir_path(),
                None => return err::dir_missing_in_path(),
            }
        }
        Ok(current)
    }
}
