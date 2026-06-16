
from qcc.domain.characteristic import Characteristic
from typing import Dict

class CharacteristicDictionary:

    dictionary = Dict[int, Characteristic]

    def addCharacteristic(self, char : Characteristic):
        global dictionary
        dictionary[char.id] = char

    def getCharacteristic(self, id : int) -> Characteristic:
        global dictionary
        return dictionary[id]