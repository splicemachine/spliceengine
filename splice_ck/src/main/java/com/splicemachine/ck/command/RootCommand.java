/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.ck.command;

import com.splicemachine.ck.ConnectionSingleton;
import com.splicemachine.ck.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Scanner;

@Command(mixinStandardHelpOptions = true, name = "spliceck", description = "SpliceMachine check command suite",
        descriptionHeading = "Description:%n",
        optionListHeading = "Options:%n", subcommands = {TListCommand.class,
        TColsCommand.class, RegionOfCommand.class, TableOfCommand.class, RGetCommand.class, RPutCommand.class,
        TxListCommand.class, SListCommand.class})
class RootCommand {
    public static void main(String... args) {
        Logger.getRootLogger().setLevel(Level.OFF);
        ConnectionSingleton c = new ConnectionSingleton();

        int exitCode = 0;
        if(!args[0].equals("interactive")) {
            exitCode = new CommandLine(new RootCommand()).setExecutionStrategy(new CommandLine.RunLast()).execute(args);
        }
        else {
            Scanner command = new Scanner(System.in);
            CommandLine cl = new CommandLine(new RootCommand()).setExecutionStrategy(new CommandLine.RunLast());
            while (true) {
                System.out.print("spliceck> ");
                String s = command.nextLine().trim();

                if (s.equals("exit") || s.equals("quit") || s.equals("q") ) break;
                if (s.length() == 0) continue;

                String[] args2 = s.split(" ");
                exitCode = cl.execute(args2);
            }
            command.close();
        }

        try {
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
