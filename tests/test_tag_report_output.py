import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from qcc.reports.tag_reports.tag_report_output import TagReportOutput

class TestTagReportOutput:

    def test_setup_method(self):
        from pathlib import Path
        report = TagReportOutput()
        csv_output = Path(os.path.dirname(__file__)) / "data" / "tag_report_1207.csv"

        try:
            report.db_to_csv(csv_output, assignment_id="1207")
            print("db_to_csv FINISHED")
        except Exception as e:
            print("ERROR OCCURRED:", e)
        pass

    