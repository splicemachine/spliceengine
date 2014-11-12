/**
 * Defines the interfaces used by the SI module to access the underlying data store (i.e. HBase). Defining these as
 * interfaces allows an alternate, lightweight implementation to be used instead of HBase for rapid automated unit
 * testing.
 */
package com.splicemachine.si.data.api;