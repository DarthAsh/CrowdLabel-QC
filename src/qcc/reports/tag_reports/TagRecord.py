from qcc.reports.tagger_reports.tag_report import count_yes_no
from qcc.domain.enums import TagValue

class TagRecord:
    def __init__(self, comment_id, characteristic_id, assignments):
        self.comment_id = comment_id
        self.characteristic_id = characteristic_id
        self.assignments = assignments

    @property
    def num_taggers(self):
        return len(self.assignments)

    @property
    def num_yes(self):
        yes, _ = count_yes_no(self.assignments)
        return yes

    @property
    def num_no(self):
        _, no = count_yes_no(self.assignments)
        return no

    @property
    def num_skipped(self):
        skipped = 0
        for assignment in self.assignments:
            if getattr(assignment, "value", None) == TagValue.SKIP:
                skipped += 1
        return skipped