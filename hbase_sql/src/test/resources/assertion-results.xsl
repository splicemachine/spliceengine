<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
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
                                <xsl:value-of select="current()/@s"/>
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
