/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.kelvin.stream.actions;

import com.google.common.base.Function;
import com.google.common.io.Closeables;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.kelvin.stream.actions.utils.Pair;

/**
 *
 * @author Shashikiran
 */
public class StreamActionTest
{

    static final Logger sLogger = Logger.getLogger(StreamActionTest.class.getName());
    Function<Pair<InputStream, ? extends OutputStream>, Boolean> copyFunction
            = new Function<Pair<InputStream, ? extends OutputStream>, Boolean>()
            {
                @Override
                public Boolean apply(Pair<InputStream, ? extends OutputStream> input)
                {
                    try {
                        IOUtils.copy(input.getFirst(), input.getSecond());
                        input.getFirst().close();
                        input.getSecond().close();
                    } catch (IOException ex) {
                        sLogger.log(Level.WARNING, "Exception in copying bytes!", ex);
                        return false;
                    } finally {
                        Closeables.closeQuietly(input.getFirst());
                        Closeables.closeQuietly(input.getSecond());
                    }
                    return true;
                }
            };
    Function<Pair<InputStream, ? extends OutputStream>, Boolean> lowerCaseFunction
            = new Function<Pair<InputStream, ? extends OutputStream>, Boolean>()
            {
                @Override
                public Boolean apply(Pair<InputStream, ? extends OutputStream> input)
                {
                    try {
                        InputStream is = input.getFirst();
                        OutputStream os = input.getSecond();
                        int nextChar;
                        while (-1 != (nextChar = is.read())) {
                            os.write(nextChar < 'A' || nextChar > 'Z' ? nextChar : 'a' + (nextChar - 'A'));
                        }
                        input.getFirst().close();
                        input.getSecond().close();
                    } catch (IOException ex) {
                        sLogger.log(Level.WARNING, "Exception in copying bytes!", ex);
                        return false;
                    } finally {
                        Closeables.closeQuietly(input.getFirst());
                        Closeables.closeQuietly(input.getSecond());
                    }
                    return true;
                }
            };
    Function<Pair<InputStream, ? extends OutputStream>, Boolean> alphaEaterFunction
            = new Function<Pair<InputStream, ? extends OutputStream>, Boolean>()
            {
                @Override
                public Boolean apply(Pair<InputStream, ? extends OutputStream> input)
                {
                    try {
                        InputStream is = input.getFirst();
                        OutputStream os = input.getSecond();
                        int nextChar;
                        while (-1 != (nextChar = is.read())) {
                            if ((nextChar < 'A' || nextChar > 'Z')
                            && (nextChar < 'a' || nextChar > 'z')) {
                                os.write(nextChar);
                            }
                        }
                    } catch (IOException ex) {
                        sLogger.log(Level.WARNING, "Exception in copying bytes!", ex);
                        return false;
                    } finally {
                        Closeables.closeQuietly(input.getFirst());
                        Closeables.closeQuietly(input.getSecond());
                    }
                    return true;
                }
            };
    ExecutorService executorService;

    @Before
    public void setUp()
    {
        executorService = Executors.newFixedThreadPool(10);
    }

    @Test
    public void testSingleStreamAction() throws InterruptedException, ExecutionException, IOException
    {
        StreamAction<Boolean> action = new StreamAction<>(copyFunction, 1);
        action.connect(new ByteArrayInputStream(new byte[]{'K', 'E', 'L', 'V', 'I', 'N'}));
        Future<Boolean> future = executorService.submit(action);
        List<String> lines = IOUtils.readLines(action.getActionStream());
        Assert.assertEquals("KELVIN", lines.get(0));
        Assert.assertEquals(Boolean.TRUE, future.get());
    }

    @Test
    public void testMultipleStreamActions() throws InterruptedException, ExecutionException, IOException
    {
        StreamAction<Boolean> action1 = new StreamAction<>(copyFunction, 1);
        StreamAction<Boolean> action2 = new StreamAction<>(lowerCaseFunction, 1);
        StreamAction<Boolean> action3 = new StreamAction<>(alphaEaterFunction, 1);

        action1.connect(new ByteArrayInputStream(new byte[]{'K', 'E', 'L', 'V', 'I', 'N'}));
        action1.pipeTo(action2).pipeTo(action3);

        Future<Boolean> future1 = executorService.submit(action1);
        Future<Boolean> future2 = executorService.submit(action2);
        Future<Boolean> future3 = executorService.submit(action3);
        List<String> lines = IOUtils.readLines(action3.getActionStream());
        Assert.assertTrue(lines.isEmpty());
        Assert.assertEquals(Boolean.TRUE, future1.get());
        Assert.assertEquals(Boolean.TRUE, future2.get());
        Assert.assertEquals(Boolean.TRUE, future3.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPipeToSameAction() throws InterruptedException, ExecutionException, IOException
    {
        StreamAction<Boolean> action1 = new StreamAction<>(copyFunction, 1);

        action1.connect(new ByteArrayInputStream(new byte[]{'K', 'E', 'L', 'V', 'I', 'N'}));
        action1.pipeTo(action1);
        Future<Boolean> future1 = executorService.submit(action1);
        List<String> lines = IOUtils.readLines(action1.getActionStream());
        Assert.assertEquals("KELVIN", lines.get(0));
        Assert.assertEquals(Boolean.TRUE, future1.get());
    }

    @Test
    public void testSingleStreamActionWithBadStream() throws InterruptedException, ExecutionException, IOException
    {
        StreamAction<Boolean> action = new StreamAction<>(copyFunction, 1);
        action.connect(new InputStream()
        {
            @Override
            public int read() throws IOException
            {
                throw new IOException("don't ask for data!");
            }
        });
        Future<Boolean> future = executorService.submit(action);
        IOUtils.readLines(action.getActionStream());
        Assert.assertEquals(Boolean.FALSE, future.get());
    }

    @Test
    public void testMultipleStreamActionsWithBadStream() throws InterruptedException, ExecutionException, IOException
    {
        StreamAction<Boolean> action1 = new StreamAction<>(copyFunction, 1);
        StreamAction<Boolean> action2 = new StreamAction<>(lowerCaseFunction, 1);
        StreamAction<Boolean> action3 = new StreamAction<>(alphaEaterFunction, 1);

        action1.connect(new InputStream()
        {
            @Override
            public int read() throws IOException
            {
                throw new IOException("don't ask for data!");
            }
        });

        action1.pipeTo(action2).pipeTo(action3);

        Future<Boolean> future1 = executorService.submit(action1);
        Future<Boolean> future2 = executorService.submit(action2);
        Future<Boolean> future3 = executorService.submit(action3);
        Assert.assertEquals(0, IOUtils.readLines(action3.getActionStream()).size());
        Assert.assertEquals(Boolean.FALSE, future1.get());
        Assert.assertEquals(Boolean.TRUE, future2.get());
        Assert.assertEquals(Boolean.TRUE, future3.get());
    }

    @Test(expected = NullPointerException.class)
    public void testNullStream()
    {
        StreamAction<Boolean> action = new StreamAction<>(copyFunction);
        action.connect(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPipeToNullAction()
    {
        StreamAction<Boolean> action = new StreamAction<>(copyFunction);
        action.pipeTo(null);
    }

    @Test
    public void testSinkAction() throws Exception
    {
        StreamAction<Boolean> action = new StreamAction<>(copyFunction);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] data = new byte[]{'K', 'E', 'L', 'V', 'I', 'N'};
        action.connect(new ByteArrayInputStream(data));
        action.pipeToSink(baos);
        action.call();
        Assert.assertTrue(Arrays.equals(data, baos.toByteArray()));
    }

    @Test(expected = IllegalStateException.class)
    public void testPipeFromSinkAction() throws Exception
    {
        StreamAction<Boolean> action1 = new StreamAction<>(copyFunction);
        StreamAction<Boolean> action2 = new StreamAction<>(copyFunction);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] data = new byte[]{'K', 'E', 'L', 'V', 'I', 'N'};
        action1.connect(new ByteArrayInputStream(data));
        action1.pipeToSink(baos);
        action1.pipeTo(action2);
    }
}
