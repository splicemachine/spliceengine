<?xml version="1.0" encoding="UTF-8"?>
<!--
  ~ Copyright 2012 - 2016 Splice Machine, Inc.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License"); you may not use
  ~ this file except in compliance with the License. You may obtain a copy of the
  ~ License at
  ~
  ~ http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software distributed
  ~ under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
  ~ CONDITIONS OF ANY KIND, either express or implied. See the License for the
  ~ specific language governing permissions and limitations under the License.
  -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd" version="2.0">
    <xsl:template match="/">
        <html>
            <style type="text/css">
                body { font-family: Arial, Helvetica, sans-serif; }
                table, th, td { border-collapse: collapse; border: 1px solid #333333;}
                tr:nth-child(even) {background-color: #f2f2f2}
                th, td { padding: 3px; text-align: left; }
                th { background-color: #f6a704; color: #333333; }
            </style>
            <body>
                <h2>Test Results</h2>
                <table>
                    <thread>
                        <tr>
                            <th>test label</th>
                            <th>thread name</th>
                            <th>pass</th>
                            <th>start time</th>
                            <th>elapsed time(ms)</th>
                            <th>response code</th>
                            <th>failure message</th>
                        </tr>
                    </thread>
                    <xsl:for-each select="testResults/sample">
                        <tr>
                            <td>
                                <xsl:value-of select="current()/@lb"/>
                            </td>
                            <td>
                                <xsl:value-of select="current()/@tn"/>
                            </td>
                            <td>
                                <xsl:attribute name="style">
                                    <xsl:if test="current()/@s = 'false'">
                                        background-color: #FFB3B3;
                                        color: #800000;
                                    </xsl:if>
                                    <xsl:if test="current()/@s = 'true'">
                                        background-color: #CCFFCC;
                                        color: #003300;
                                    </xsl:if>
                                </xsl:attribute>
                                <xsl:value-of select="current()/@s"/>
                            </td>
                            <td>
                                <xsl:variable name="starttimemillis" select="current()/@ts"/>
                                <xsl:variable name="starttime" select="xs:dateTime(&quot;1970-01-01T00:00:00&quot;) + $starttimemillis * xs:dayTimeDuration(&quot;PT0.001S&quot;)"/>
                                <xsl:value-of select="format-dateTime($starttime, &quot;[M01]/[D01]/[Y0001] [H01]:[m01]:[s01].[f001]&quot;)"/>
                            </td>
                            <td>
                                <xsl:value-of select="current()/@t"/>
                            </td>
                            <td>
                                <xsl:value-of select="current()/@rc"/>
                            </td>
                            <td>
                                <xsl:if test="assertionResult/failureMessage">
                                    <pre>
                                        <xsl:for-each select="assertionResult">
                                            <xsl:value-of select="failureMessage"/>
                                        </xsl:for-each>
                                    </pre>
                                </xsl:if>
                            </td>
                        </tr>
                    </xsl:for-each>
                </table>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>
