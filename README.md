Content-addressable data lake
=============================

*WIP*

Storing any kind of metadata in the name/path of a file brings mostly disadvantages.
Let's put all files in one directory, and name it with a hash of its content ([Content-addressable storage](https://en.wikipedia.org/wiki/Content-addressable_storage)).
Then, let's keep all related metadata in a database for easy manipulation.

The problem with storing metadata as paths:
1. Path is hierarchical while metadata are multidimensional,
   one has to choose the order of dimensions.
2. Paths are one-to-one to content, but the same content can correspond to multiple metadata
3. Paths are either too long to be nice to write by hand (hive-partitioning) or opaque,
   requiring additional knowledge how to interpret it.
4. Paths are string-typed.
5. Querying available files is tedious, requires listing and parsing the paths.
6. **Any kind of change is extremely tedious to do**. Requires moving around files and destructive
   operations or duplication.
7. When two files match the same metadata, it is usually solved with some ugly suffix `_2`.
   When new versions of files appear, they are irreversibly overwritten.

Data lakes partially solve this because they need to avoid expensive file listing operations
with huge numbers of files. But they go only half-way, all the metadata are stored somewhere
in a database, but the paths still carry some semantic content, it usually contains 
some kind of date, name of dataset, or whatever.
This solves some points, some not (1, 6, 7), and brings additional problems with transactionality, and
the metadata are now in two places, impossible to keep in sync.

I find the point 6 the most pressing.
**Keeping any order in your data is only as hard as making changes to their structure.**
The ever-repeating story is that you
1. gather some data, name it somehow, partition it somehow
2. realize that there are additional parameters you did not consider, the partitioning is not exhaustive,
   you chose the name of the dataset wrongly, etc.
3. make the dataset with other values an extra dataset alongside the first one OR go through the pain
   of restructuring the first dataset to accomodate this newly-discovered feature of the data, knowing
   that you will do this again the moment you discover a new parameter
4. keep around many GBs of backups in case you did a mistake during the restructuralisation or pray
   that you didn't

**It is mandatory that you are able to restructuralize the data easily if you want to keep any order in it**.
You must be able to present the new structure while keeping backwards compatibility, without duplication.

We only got used to not having these perks, because decades ago people decided that paths are the perfect
level of abstraction at which people should interact with the filesystem.
Anything below that is flat and addressed by arbitrary numbers.
Let's move it up one level and make paths flat and addressed by uninformative numbers.

In the scenario above, had we content-addressed our files, we could have written one SQL statement to transfer our metadata
into a new catalog and keep the old one basically for free.

Most of the advantages would already come with arbitrary addresses, but then we can get for free
some other advantages by making the path related to a hash of the content.


There are many technologies that use content addressing, but it is always under the hood.
If they are file-oriented, they still try to present a path-based interface,
or they are very-domain specific and implement most interaction in a special-purpose
client (photo managers, bibliography managers).
The data catalog and hashed filenames are never exposed to the user.


For use-cases where the user is computer-literate, such as data science, content-addressability
brings mostly joy. When the user is no longer looking at icons in windows and clicking on them,
the convenience of informative paths pales in the comparison with easy manipulation of metadata
in a format which is made for it -- databases.


Of course, there are some disadvantages. The filename -> metadata mapping is not obvious anymore,
so deletion of old files is problematic and needs a dedicated garbage collector.

How to use this package
-----------------------

This project is a simple python implementation to be used for local medium size data, although
ready for any fsspec+sqlalchemy backend (e.g. s3 + postgres).
I have not investigated any scaling limits, but I have no suspicion that the same principle
could not be used for bigger data.

TODO

Acknowledgement
---------------

I don't think any of these ideas are new, though I could not anyone talking about it.
The biggest inspiration is coming from [nix](nixos.org). I recommend reading [Nix – taming Unix with functional programming](https://www.tweag.io/blog/2022-07-14-taming-unix-with-nix)
and the linked article [Imposing a Memory Management Discipline on Software Deployment](https://edolstra.github.io/pubs/immdsd-icse2004-final.pdf).
How about imposing a memory management discipline on user data?
