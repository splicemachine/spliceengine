package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.derby.ddl.DDLWatcher;
import com.splicemachine.job.ZkTaskMonitor;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class SpliceDriverTest {

    /* This would periodically fail prior to DB-2476 */
    @Test
    public void registerService_alwaysStartsPassedService() throws InterruptedException {

        // given
        final SpliceDriver spliceDriver = createTestSpliceDriver();
        final List<Service> services = Lists.newArrayList(
                mockService(), mockService(), mockService(), mockService(), mockService(), mockService(), mockService()
        );


        // when -- we call registerService() and startServices() in separate threads
        List<Thread> threads = Lists.newArrayList();

        threads.add(new Thread() {
            @Override
            public void run() {
                for (Service service : services) {
                    spliceDriver.registerService(service);
                }
            }
        });
        threads.add(new Thread() {
            @Override
            public void run() {
                spliceDriver.startServices();
            }
        });

        Collections.shuffle(threads);
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        // then -- start is always invoked on the registered services
        for (Service service : services) {
            verify(service, times(1)).start();
        }
    }

    private SpliceDriver createTestSpliceDriver() {
        ZkTaskMonitor taskMonitor = mock(ZkTaskMonitor.class);
        WriteCoordinator writeCoordinator = mock(WriteCoordinator.class);
        DDLWatcher ddlWatcher = mock(DDLWatcher.class);
        return new SpliceDriver(taskMonitor, writeCoordinator, ddlWatcher);
    }

    private Service mockService() {
        Service service = mock(Service.class);
        when(service.start()).thenReturn(true);
        return service;
    }

}