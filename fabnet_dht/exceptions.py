

class FSHashRangesException(Exception):
    PREFIX = 'FSHashRange'
    def __init__(self, err):
        prefix = '[%s] '%self.PREFIX
        if not err.startswith(prefix):
            err = prefix + err
        super(FSHashRangesException, self).__init__(err)

class FSHashRangesNotFound(FSHashRangesException):
    PREFIX = 'FSHashRangeNotFound'

class FSHashRangesNoData(FSHashRangesException):
    PREFIX = 'FSHashRangeNoData'

class FSHashRangesInvalidDataBlock(FSHashRangesException):
    PREFIX = 'InvalidDataBlock'

class FSHashRangesOldDataDetected(FSHashRangesException):
    PREFIX = 'OldDataBlock'

class FSHashRangesPermissionDenied(FSHashRangesException):
    PREFIX = 'DBPermissionDenied'

class FSHashRangesNoFreeSpace(FSHashRangesException):
    PREFIX = 'FSHashRangeNoFreeSpace'
    
