/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.kelvin.stream.actions;

/**
 *
 * @author Shashikiran
 */
public class StreamActionIntializationException extends RuntimeException
{

    public StreamActionIntializationException()
    {
    }

    public StreamActionIntializationException(String message)
    {
        super(message);
    }

    public StreamActionIntializationException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public StreamActionIntializationException(Throwable cause)
    {
        super(cause);
    }

    public StreamActionIntializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
