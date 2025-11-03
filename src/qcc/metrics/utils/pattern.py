from qcc.domain.enums import TagValue


# NOTE: Pattern class implementation written below, do not know where to use it yet
# class Pattern:
#     def __init__(self, sequence_str: str):
#         allowed = ["Y", "N"]
#         sequence_str = sequence_str.upper()
#         unique_chars = set(sequence_str)
#         if len(unique_chars) > len(allowed):
#             raise TypeError("Not a valid pattern - contains more types of characters than allowed!")
        
#         if not unique_chars.issubset(allowed):
#             raise TypeError("Not a valid pattern - contains not allowed characters!")
        
#         self.value = sequence_str

class PatternCollection:
    # Added NYYN as four_length_pattern as well
    one_length_patterns = ["Y", "N"]

    two_length_patterns = ["YN"]

    three_length_patterns = ["YNY", "YNN"]

    four_length_patterns = ["YNNY", "YYYN", "YNNN"]

    @classmethod
    def return_all_patterns(cls):
        all_patterns = []

        all_patterns.extend(cls.two_length_patterns)
        all_patterns.extend(cls.three_length_patterns)
        all_patterns.extend(cls.four_length_patterns)

        return all_patterns

# YY, NN, YNY, NYN, YNYN, YNNY)