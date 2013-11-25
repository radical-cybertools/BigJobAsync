import saga

srcdir = saga.filesystem.Directory(
    "sftp://india.futuregrid.org/tmp/ole.weidner/src/",
    saga.filesystem.CREATE_PARENTS)

files  = []
for i in range (0, 400) :
   print "open test_%02d.dat" % i
   files.append (srcdir.open ("test_%02d.dat" % i, saga.filesystem.CREATE))

tgtdir = saga.filesystem.Directory ("file://localhost/tmp/ole.weidner/tgt/",
                                   saga.filesystem.CREATE_PARENTS)
for f in files :
   print "copy %s file://localhost/tmp/ole.weidner/tgt/" % f.url
   f.copy ("file://localhost/tmp/ole.weidner/tgt/")

