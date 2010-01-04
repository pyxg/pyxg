from Foundation import *
from PyObjCTools import Conversion

def myNewParser(result):
    tempResult = NSString.alloc().initWithString_(unicode(result))
    print tempResult
    resultDict = tempResult.nsstring().propertyListFromStringsFileFormat()
    return Conversion.pythonCollectionFromPropertyList(resultDict)

