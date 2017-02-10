/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.tests.upgradeTests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import com.splicemachine.db.iapi.services.info.ProductVersionHolder;

/**
 * <p>
 * A Derby version.
 * </p>
 */
public class Version implements Comparable
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final int EXPECTED_LEG_COUNT = 4;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private int[] _legs;
    private String _key;
    private String _branchID;

    // we keep one class loader per version so that we don't have an explosion
    // of class loaders for redundant versions
    private static HashMap _classLoaders = new HashMap();
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Construct a version from four legs */
    public Version( int major, int minor, int fixpack, int bugversion )
    {
        this( new int[] { major, minor, fixpack, bugversion } );
    }
    
    /** Construct a version from its legs */
    public Version( int[] legs )
    {
        constructorMinion( legs );
    }

    /** Construct from a Derby ProductVersionHolder  */
    public Version( ProductVersionHolder pvh )
    {
        constructorMinion( getLegs( pvh ) );
    }
    private void constructorMinion( int[] legs )
    {
        if ( legs == null ) { legs = new int[] {}; }
        int count = legs.length;

        if ( count != EXPECTED_LEG_COUNT )
        {
            throw new IllegalArgumentException( "Expected " + EXPECTED_LEG_COUNT + " legs but only saw " + count );
        }

        _legs = new int[ count ];
        System.arraycopy(legs, 0, _legs, 0, count);

        makeKey();
    }
    private int[] getLegs( ProductVersionHolder pvh )
    {
        int[] result = new int[ EXPECTED_LEG_COUNT ];
        int   idx = 0;

        result[ idx++ ] = pvh.getMajorVersion();
        result[ idx++ ] = pvh.getMinorVersion();
        result[ idx++ ] = pvh.getMaintVersion() / ProductVersionHolder.MAINT_ENCODING;
        result[ idx++ ] = pvh.getMaintVersion() % ProductVersionHolder.MAINT_ENCODING;

        return result;
     }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Pretty-print this version.
     * </p>
     */
    public String toString()
    {
        return _key;
    }

    /**
     * <p>
     * Pretty-print the branch id, that is, the major + minor legs of the Version.
     * </p>
     */
    public String getBranchID()
    {
        if ( _branchID == null )
        {
            _branchID = Integer.toString(_legs[ 0 ]) + '.' + Integer.toString(_legs[ 1 ]);
        }

        return _branchID;
    }

    /**
     * <p>
     * Get a class loader for this version.
     * </p>
     */
    public ClassLoader getClassLoader()
    {
        ClassLoader retval = (ClassLoader) _classLoaders.get( _key );
        if ( retval != null ) { return retval; }
        
        addClassLoader( );
        
        return (ClassLoader) _classLoaders.get( _key );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Comparable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public int compareTo( Object other )
    {
        if ( other == null ) { return 1; }
        if ( !(other instanceof Version) ) { return 1; }

        Version that = (Version) other;

        for ( int i = 0; i < EXPECTED_LEG_COUNT; i++ )
        {
            int result = this._legs[ i ] - that._legs[ i ];

            if ( result != 0 ) { return result; }
        }

        return 0;
    }

    public boolean equals( Object other ) { return ( compareTo( other ) == 0 ); }
    public int hashCode() { return toString().hashCode(); }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Add the class loader for this version if it doesn't already exist.
     * </p>
     */
    private void addClassLoader( )
    {

    }

    /**
     * <p>
     * Make the key for looking up our class loader.
     * </p>
     */
    private void makeKey()
    {
        StringBuffer buffer = new StringBuffer();
        int          legCount = _legs.length;

        for ( int i = 0; i < legCount; i++ )
        {
            if ( i > 0 ) { buffer.append( '.' ); }
            buffer.append( _legs[ i ] );
        }

        _key = buffer.toString();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This is a sequence of Versions. It is the caller's responsibility to
     * determine whether the Versions are in sort order.
     * </p>
     */
    public static final class Trajectory implements Comparable
    {
        private Version[] _versions;

        /**
         * <p>
         * Construct from a list of Versions.
         * </p>
         */
        public Trajectory( ArrayList versionList )
        {
            if ( versionList == null ) { versionList = new ArrayList(); }

            Version[] versions = new Version[ versionList.size() ];
            versionList.toArray( versions );

            constructorMinion( versions );
        }

        /**
         * <p>
         * Construct from an array of Versions.
         * </p>
         */
        public Trajectory( Version[] versions )
        {
            if ( versions == null ) { versions = new Version[ 0 ]; }

            constructorMinion( versions );
        }

        private void constructorMinion( Version[] versions )
        {
            int count = versions .length;
            _versions = new Version[ count ];

            System.arraycopy(versions, 0, _versions, 0, count);
        }

        /**
         * <p>
         * Sort this Trajectory so that the Versions are arranged in ascending
         * order. Returns this Trajectory after the sort.
         * </p>
         */
        public Trajectory sort()
        {
            Arrays.sort( _versions );

            return this;
        }

        public int getVersionCount() { return _versions.length; }
        public Version getVersion( int idx ) { return _versions[ idx ]; }

        /**
         * <p>
         * Return the ending Version of this Trajectory.
         * </p>
         */
        public Version getEndingVersion()
        {
            return getVersion( getVersionCount() -1 );
        }
        
        /**
         * <p>
         * Return true if this Trajectory starts at the desired Version.
         * </p>
         */
        public boolean startsAt( Version candidate )
        {
            return ( getVersion( 0 ).equals( candidate ) );
        }
        
        /**
         * <p>
         * Return true if this Trajectory starts at the desired branch.
         * </p>
         */
        public boolean startsAt( String branchID )
        {
            return ( getVersion( 0 ).getBranchID().equals( branchID ) );
        }
        
        /**
         * <p>
         * Return true if this Trajectory contains the desired Version.
         * </p>
         */
        public boolean contains( Version candidate )
        {
            int count = getVersionCount();
            for ( int i = 0; i < count; i++ )
            {
                if ( getVersion( i ).equals( candidate ) ) { return true; }
            }
            
            return false;
        }
        
        /**
         * <p>
         * Return true if this Trajectory contains a version from the desired branch.
         * </p>
         */
        public boolean contains( String branchID )
        {
            int count = getVersionCount();
            for ( int i = 0; i < count; i++ )
            {
                if ( getVersion( i ).getBranchID().equals( branchID ) ) { return true; }
            }
            
            return false;
        }
        
        /**
         * <p>
         * Return true if this Trajectory ends at the desired Version.
         * </p>
         */
        public boolean endsAt( Version candidate )
        {
            return ( getVersion( getVersionCount() - 1 ).equals( candidate ) );
        }
        
        /**
         * <p>
         * Return true if this Trajectory ends at the desired branch.
         * </p>
         */
        public boolean endsAt( String branchID )
        {
            return ( getVersion( getVersionCount() - 1 ).getBranchID().equals( branchID ) );
        }
        
        public String toString()
        {
            StringBuffer buffer = new StringBuffer();
            int          count = _versions.length;

            for ( int i = 0; i < count; i++ )
            {
                if ( i > 0 ) { buffer.append( " -> " ); }
                buffer.append( _versions[ i ].toString() );
            }

            return buffer.toString();
        }

        public int compareTo( Object other )
        {
            if ( other == null ) { return -1; }
            if ( !(other instanceof Trajectory) ) { return -1; }

            Trajectory that = (Trajectory) other;
            int           thisLength = this.getVersionCount();
            int           thatLength = that.getVersionCount();
            int           minLength = thisLength < thatLength ? thisLength : thatLength;

            for ( int i = 0; i < minLength; i++ )
            {
                int result = this.getVersion( i ).compareTo( that.getVersion( i ) );
                if ( result != 0 ) { return result; }
            }

            return thisLength - thatLength;
        }

        public boolean equals( Object other ) { return ( compareTo( other ) == 0 ); }

        public int hashCode() { return toString().hashCode(); }
    }


}
