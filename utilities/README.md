utilities
=========

Generic Utilities upon which SpliceMachine relies

This library encompasses general-purpose tools which do not exist within
the Java Standard Library or any of the common toolkits (i.e. guava), or
which exist, but perform poorly for the use case of SpliceMachine.

The intention of this library is 

* To be useful in a multitude of different settings throughout SpliceMachine
* To be high performance
* Have a low footprint
* Does not depends on specific libraries for its functionality 

In particular, we don't want any HBase-specific (or Hadoop-specific) code
in this library, as it violated #4.

Why do we care about #4 so much? Well, part of the purpose of creating
this library is to avoid having to merge general-purpose utilities into
400 different branches of splice engine's core codebase, so adding in
Hbase-specific dependencies will mean we have to keep 400 different
versions of this library also, which is not pleasant. So keep
HBase dependencies out! 

If you want to include something in here that has an HBase-specific
implementation, then be sure and use interfaces! We have a 
language that supports polymorphism for a REASON.

