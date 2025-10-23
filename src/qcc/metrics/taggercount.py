from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Set

from qcc.domain.tagger import Tagger
from qcc.domain.tagassignment import TagAssignment
from qcc.domain.comment import Comment
from qcc.domain.characteristic import Characteristic

def count_taggers_who_tagged_this_characteristic_for_this_comment(tag_assignments: List[TagAssignment], target_comment: Comment, target_characteristic: Characteristic):
    # need other instances of tagger outside of the current tag assignment object
    # db retrieval?
    unique_taggers: Set[Tagger] = set()
    
    for assignment in tag_assignments:
        if (assignment.comment_assigned_for == target_comment and assignment.characteristic_assigned_for == target_characteristic):
            unique_taggers.add(assignment.tagger_who_assigned)

    return len(unique_taggers)