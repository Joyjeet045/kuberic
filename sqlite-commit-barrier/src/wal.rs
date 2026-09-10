pub const WAL_HEADER_SIZE: usize = 32;
pub const FRAME_HEADER_SIZE: usize = 24;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct WalLayout {
    pub page_size: u32,
}

impl WalLayout {
    pub fn frame_size(self) -> u64 {
        FRAME_HEADER_SIZE as u64 + u64::from(self.page_size)
    }

    pub fn frame_header_offset(self, index: u64) -> u64 {
        WAL_HEADER_SIZE as u64 + index * self.frame_size()
    }

    pub fn frame_index_containing(self, offset: u64) -> Option<u64> {
        if offset < WAL_HEADER_SIZE as u64 {
            return None;
        }
        Some((offset - WAL_HEADER_SIZE as u64) / self.frame_size())
    }
}

pub fn parse_wal_header(bytes: &[u8]) -> Option<WalLayout> {
    if bytes.len() < WAL_HEADER_SIZE {
        return None;
    }
    let magic = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if magic != 0x377f_0682 && magic != 0x377f_0683 {
        return None;
    }
    let page_size = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    if page_size < 512 || !page_size.is_power_of_two() {
        return None;
    }
    Some(WalLayout { page_size })
}

pub fn commit_page_count(frame_header: &[u8]) -> Option<u32> {
    if frame_header.len() < 8 {
        return None;
    }
    let count = u32::from_be_bytes([
        frame_header[4],
        frame_header[5],
        frame_header[6],
        frame_header[7],
    ]);
    (count != 0).then_some(count)
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StageOutcome {
    Buffered,
    FlushRequired,
}

#[derive(Debug, Default)]
pub struct WalStage {
    layout: Option<WalLayout>,
    start: u64,
    bytes: Vec<u8>,
    commit_pages: Option<u32>,
}

impl WalStage {
    pub fn layout(&self) -> Option<WalLayout> {
        self.layout
    }

    pub fn set_layout(&mut self, layout: WalLayout) {
        self.layout = Some(layout);
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn commit_pages(&self) -> Option<u32> {
        self.commit_pages
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn end(&self) -> u64 {
        self.start + self.bytes.len() as u64
    }

    pub fn accept(&mut self, offset: u64, data: &[u8]) -> StageOutcome {
        if offset == 0
            && let Some(layout) = parse_wal_header(data)
        {
            self.layout = Some(layout);
        }
        if self.bytes.is_empty() {
            self.start = offset;
            self.bytes.extend_from_slice(data);
            self.rescan();
            return StageOutcome::Buffered;
        }
        if offset == self.end() {
            self.bytes.extend_from_slice(data);
            self.rescan();
            return StageOutcome::Buffered;
        }
        if offset >= self.start && offset + data.len() as u64 <= self.end() {
            let at = (offset - self.start) as usize;
            self.bytes[at..at + data.len()].copy_from_slice(data);
            self.rescan();
            return StageOutcome::Buffered;
        }
        StageOutcome::FlushRequired
    }

    pub fn read_overlay(&self, offset: u64, out: &mut [u8]) -> bool {
        if self.bytes.is_empty() {
            return false;
        }
        let read_end = offset + out.len() as u64;
        if read_end <= self.start || offset >= self.end() {
            return false;
        }
        let overlap_start = offset.max(self.start);
        let overlap_end = read_end.min(self.end());
        let dst = (overlap_start - offset) as usize;
        let src = (overlap_start - self.start) as usize;
        let len = (overlap_end - overlap_start) as usize;
        out[dst..dst + len].copy_from_slice(&self.bytes[src..src + len]);
        offset >= self.start && read_end <= self.end()
    }

    pub fn take(&mut self) -> (u64, Vec<u8>) {
        self.commit_pages = None;
        let start = self.start;
        self.start = 0;
        (start, std::mem::take(&mut self.bytes))
    }

    pub fn discard_from(&mut self, size: u64) {
        if size <= self.start {
            self.take();
            return;
        }
        if size < self.end() {
            self.bytes.truncate((size - self.start) as usize);
            self.rescan();
        }
    }

    fn rescan(&mut self) {
        self.commit_pages = None;
        let Some(layout) = self.layout else {
            return;
        };
        let frame_size = layout.frame_size();
        let first = layout
            .frame_index_containing(self.start)
            .unwrap_or_default();
        let mut index = first;
        loop {
            let header = layout.frame_header_offset(index);
            if header + FRAME_HEADER_SIZE as u64 > self.end() {
                break;
            }
            if header >= self.start {
                let at = (header - self.start) as usize;
                if let Some(pages) = commit_page_count(&self.bytes[at..at + FRAME_HEADER_SIZE])
                    && header + frame_size <= self.end()
                {
                    self.commit_pages = Some(pages);
                }
            }
            index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: u32 = 4096;

    fn layout() -> WalLayout {
        WalLayout { page_size: PAGE }
    }

    fn wal_header() -> Vec<u8> {
        let mut header = vec![0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&0x377f_0682u32.to_be_bytes());
        header[8..12].copy_from_slice(&PAGE.to_be_bytes());
        header
    }

    fn frame(page_number: u32, commit_pages: u32) -> Vec<u8> {
        let mut bytes = vec![0u8; FRAME_HEADER_SIZE + PAGE as usize];
        bytes[0..4].copy_from_slice(&page_number.to_be_bytes());
        bytes[4..8].copy_from_slice(&commit_pages.to_be_bytes());
        bytes
    }

    #[test]
    fn a_wal_header_yields_the_page_size() {
        assert_eq!(parse_wal_header(&wal_header()), Some(layout()));
    }

    #[test]
    fn a_foreign_header_is_rejected() {
        let mut header = wal_header();
        header[0] = 0;
        assert_eq!(parse_wal_header(&header), None);
        assert_eq!(parse_wal_header(&[0u8; 8]), None);
    }

    #[test]
    fn a_non_power_of_two_page_size_is_rejected() {
        let mut header = wal_header();
        header[8..12].copy_from_slice(&3000u32.to_be_bytes());
        assert_eq!(parse_wal_header(&header), None);
    }

    #[test]
    fn only_a_non_zero_page_count_marks_a_commit() {
        assert_eq!(commit_page_count(&frame(1, 0)[..FRAME_HEADER_SIZE]), None);
        assert_eq!(
            commit_page_count(&frame(1, 7)[..FRAME_HEADER_SIZE]),
            Some(7)
        );
    }

    #[test]
    fn a_transaction_without_a_commit_frame_is_not_publishable() {
        let mut stage = WalStage::default();
        assert_eq!(stage.accept(0, &wal_header()), StageOutcome::Buffered);
        assert_eq!(
            stage.accept(WAL_HEADER_SIZE as u64, &frame(1, 0)),
            StageOutcome::Buffered
        );
        assert_eq!(stage.commit_pages(), None);
    }

    #[test]
    fn a_commit_frame_makes_the_transaction_publishable() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        stage.accept(WAL_HEADER_SIZE as u64, &frame(1, 0));
        let second = WAL_HEADER_SIZE as u64 + layout().frame_size();
        stage.accept(second, &frame(2, 2));
        assert_eq!(stage.commit_pages(), Some(2));
    }

    #[test]
    fn a_commit_frame_is_not_publishable_until_its_page_data_arrives() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        let commit = frame(1, 1);
        stage.accept(WAL_HEADER_SIZE as u64, &commit[..FRAME_HEADER_SIZE]);
        assert_eq!(stage.commit_pages(), None);

        stage.accept(
            WAL_HEADER_SIZE as u64 + FRAME_HEADER_SIZE as u64,
            &commit[FRAME_HEADER_SIZE..],
        );
        assert_eq!(stage.commit_pages(), Some(1));
    }

    #[test]
    fn a_discontiguous_write_requires_a_flush() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        assert_eq!(
            stage.accept(4096 * 10, &[1, 2, 3]),
            StageOutcome::FlushRequired
        );
    }

    #[test]
    fn an_overwrite_inside_the_stage_is_applied_in_place() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        stage.accept(WAL_HEADER_SIZE as u64, &frame(1, 0));
        assert_eq!(stage.commit_pages(), None);

        let commit = frame(1, 3);
        assert_eq!(
            stage.accept(WAL_HEADER_SIZE as u64, &commit[..FRAME_HEADER_SIZE]),
            StageOutcome::Buffered
        );
        assert_eq!(stage.commit_pages(), Some(3));
    }

    #[test]
    fn a_read_inside_the_stage_is_served_from_it() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        let mut out = vec![0u8; 4];
        assert!(stage.read_overlay(0, &mut out));
        assert_eq!(out, 0x377f_0682u32.to_be_bytes());
    }

    #[test]
    fn a_read_outside_the_stage_is_not_served() {
        let mut stage = WalStage::default();
        stage.accept(WAL_HEADER_SIZE as u64, &[1, 2, 3, 4]);
        let mut out = vec![0u8; 4];
        assert!(!stage.read_overlay(0, &mut out));
        assert_eq!(out, [0, 0, 0, 0]);
    }

    #[test]
    fn a_partial_read_reports_that_it_is_incomplete() {
        let mut stage = WalStage::default();
        stage.accept(8, &[9, 9, 9, 9]);
        let mut out = vec![0u8; 8];
        assert!(!stage.read_overlay(4, &mut out));
        assert_eq!(out[4..8], [9, 9, 9, 9]);
    }

    #[test]
    fn taking_the_stage_clears_the_commit_marker() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        stage.accept(WAL_HEADER_SIZE as u64, &frame(1, 1));
        assert!(stage.commit_pages().is_some());

        let (start, bytes) = stage.take();
        assert_eq!(start, 0);
        assert!(!bytes.is_empty());
        assert!(stage.is_empty());
        assert_eq!(stage.commit_pages(), None);
    }

    #[test]
    fn truncating_below_the_stage_discards_it() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        stage.accept(WAL_HEADER_SIZE as u64, &frame(1, 1));
        stage.discard_from(0);
        assert!(stage.is_empty());
        assert_eq!(stage.commit_pages(), None);
    }

    #[test]
    fn truncating_inside_the_stage_removes_the_commit_frame() {
        let mut stage = WalStage::default();
        stage.accept(0, &wal_header());
        stage.accept(WAL_HEADER_SIZE as u64, &frame(1, 1));
        assert!(stage.commit_pages().is_some());

        stage.discard_from(WAL_HEADER_SIZE as u64 + 8);
        assert_eq!(stage.commit_pages(), None);
        assert_eq!(stage.end(), WAL_HEADER_SIZE as u64 + 8);
    }

    #[test]
    fn a_stage_starting_mid_wal_still_finds_its_commit_frame() {
        let mut stage = WalStage::default();
        stage.set_layout(layout());
        let third = WAL_HEADER_SIZE as u64 + 2 * layout().frame_size();
        stage.accept(third, &frame(5, 4));
        assert_eq!(stage.commit_pages(), Some(4));
    }

    #[test]
    fn a_stage_without_a_known_layout_never_reports_a_commit() {
        let mut stage = WalStage::default();
        stage.accept(WAL_HEADER_SIZE as u64, &frame(1, 1));
        assert_eq!(stage.commit_pages(), None);
    }
}
