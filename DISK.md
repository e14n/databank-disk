disk driver
-----------

The disk driver provides on-disk storage of data -- one file per
object.

Each databank is mapped to a directory; each type is a sub-directory
of the main databank directory.

Usage
=====

To create a disk databank, use the `Databank.get()` method:

    var Databank = require('databank').Databank;
    
    var db = Databank.get('disk', {dir: '/var/lib/mydatabank'});
    
The driver takes the following parameters:

* `schema`: the database schema, as described in the Databank README.
* `dir`: main directory for the databank. Default is `/var/lib/diskdatabank`.
* `mktmp`: if truthy, `dir` will be ignored, and a new temporary directory
  under `os.tmpDir()` will be made. The temporary dir will be deleted when
  the databank is disconnected.
* `mode`: creation mode for the main databank dir and its subdirs; default is `0660`.
* `hashDepth`: files under the type subdirs are stored according to a hash
  of the id; for more efficient retrieval, there are hashed subdirs under each dir.
  So the `state` object stored under `BRh1Az3` will be found at `<maindir>/state/B/BR/BRh/BRh1Az3.json`.
  This value sets the number of subdirs to use.
