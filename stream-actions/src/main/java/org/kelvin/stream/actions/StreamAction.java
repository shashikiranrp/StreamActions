/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.kelvin.stream.actions;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Callable;
import org.kelvin.stream.actions.utils.Pair;

/**
 * This type wraps an IO action which transforms the stream of bytes and write to a stream.
 * The action is represented by the function: Function<Pair<InputStream, ? extends OutputStream>, T>,
 * see {@link Function} for more information. And it is responsibility of this function to close the streams.
 * WARNING: Make sure these actions will run on different threads to avoid dead lock,
 * see {@link PipedInputStream} and {@link PipedOutputStream} for more information.
 * Usage: StreamAction a1, a2, a3;
 * ....
 * ....             // initialize objects with IO functions and streams.
 * ....
 * a1.pipeTo(a2).pipeTo(a3).pipeToSink(output stream); // building the chain.
 * ....
 * Future f1 = executor.submit(a1);
 *
 * Future f2 = executor.submit(a2);     //Make sure executor has enough number of threads (if your pool is fixed size)
 *                //to avoid deadlock when data available in any of the streams is more than buffer size of this action.
 * Future f3 = executor.submit(a3);
 * ....
 * f1.get();f2.get();f3.get(), if necessary.
 * @author Shashikiran
 */
public class StreamAction<T> implements Callable<T>
{

    private final static int DEFAULT_PIPE_SIZE = 1024;
    InputStream readStream;
    OutputStream sink;
    PipedOutputStream actionOutputStream;
    PipedInputStream actionResultStream;
    Function<Pair<InputStream, ? extends OutputStream>, T> ioFunction;

    /**
     *
     * @param ioFunction : Function representing the side effect action.
     */
    public StreamAction(Function<Pair<InputStream, ? extends OutputStream>, T> ioFunction)
    {
        this(null, ioFunction, DEFAULT_PIPE_SIZE);
    }

    /**
     *
     * @param ioFunction : Function representing the side effect action.
     * @param pipeSize  : internal buffer size of the {@link PipedInputStream}.
     */
    public StreamAction(Function<Pair<InputStream, ? extends OutputStream>, T> ioFunction,
                        int pipeSize)
    {
        this(null, ioFunction, pipeSize);
    }

    /**
     *
     * @param readStream : reading stream for the ioFunction
     * @param ioFunction : Function representing the side effect action.
     * @param pipeSize  : internal buffer size of the {@link PipedInputStream}.
     */
    public StreamAction(InputStream readStream,
                        Function<Pair<InputStream, ? extends OutputStream>, T> ioFunction,
                        int pipeSize)
    {
        Preconditions.checkNotNull(ioFunction, "ioFunction is null!");
        this.readStream = readStream;
        this.ioFunction = ioFunction;
        this.actionOutputStream = new PipedOutputStream();
        try {
            this.actionResultStream = new PipedInputStream(this.actionOutputStream, pipeSize);
        } catch (IOException ex) {
            throw new StreamActionIntializationException("error in connecting streams!", ex);
        }
    }

    /**
     *
     * @return : {@link PipedInputStream} of this action.
     */
    public PipedInputStream getActionStream()
    {
        return this.actionResultStream;
    }

    /**
     *
     * @param readStream : connects this action to the given input stream
     * @throws NullPointerException if readStream is null.
     */
    public void connect(InputStream readStream)
    {
        Preconditions.checkNotNull(readStream, "readStream is null!");
        this.readStream = readStream;
    }

    /**
     *
     * @param action : joins two actions so that this object's input stream is connected to output stream of the argument
     * @return : the second member of pipe.
     * @throws NullPointerException if action is null
     * @throws IllegalArgumentException if the action is same as this.
     */
    public StreamAction<?> pipeTo(StreamAction<?> action)
    {
        Preconditions.checkNotNull(action, "action is null");
        Preconditions.checkArgument(action != this, "can't pipe to same action!");
        Preconditions.checkState(this.sink == null, "can't pipe from sink action!");
        action.connect(getActionStream());
        return action;
    }

    public void pipeToSink(OutputStream sink)
    {
        Preconditions.checkNotNull(sink, "sink is null");
        this.sink = sink;
    }

    protected OutputStream getWriteStream()
    {
        return null == this.sink ? this.actionOutputStream : this.sink;
    }

    protected InputStream getReadStream()
    {
        return this.readStream;
    }

    /**
     *
     * @return : result of the IO computation.
     * @throws Exception if any exception happens in execution.
     */
    @Override
    public T call() throws Exception
    {
        if (null == this.readStream) {
            throw new IllegalStateException("read stream is not connected!");
        }
        return ioFunction.apply(new Pair<>(getReadStream(), getWriteStream()));
    }
}
