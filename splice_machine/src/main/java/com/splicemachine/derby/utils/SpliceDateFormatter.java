/*
 * Copyright (c) 2018 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils;

public class SpliceDateFormatter {

    private String format = null;
    private java.time.format.DateTimeFormatter formatter;

    public SpliceDateFormatter(String format) {
        this.format = format != null ? format : "yyyy-M-d";
        
        if (format != null) {
            // Replace MM with M in the date format string.
            String pattern = "([^M]*)([M])([M])([^M]*)";
            this.format = this.format.replaceFirst(pattern, "$1$2$4");
    
            // Replace dd with d in the date format string.
            pattern = "([^d]*)([d])([d])([^d]*)";
            this.format = this.format.replaceFirst(pattern, "$1$2$4");
        }
        this.formatter =
              java.time.format.DateTimeFormatter.ofPattern(this.format);
    }
    public String getFormat() {
        return format;
    }
    public java.time.format.DateTimeFormatter getFormatter() { return formatter; }
    public void setFormatter(java.time.format.DateTimeFormatter newFormatter) {formatter = newFormatter;}
}