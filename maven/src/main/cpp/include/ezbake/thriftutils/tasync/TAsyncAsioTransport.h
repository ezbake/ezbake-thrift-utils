/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

/*
 * TAsyncAsioTransport.h
 *
 *  Created on: Apr 17, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_TASYNC_TASYNCASIOTRANSPORT_H_
#define EZBAKE_THRIFTUTILS_TASYNC_TASYNCASIOTRANSPORT_H_

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/asio.hpp>
#include <boost/make_shared.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TBufferTransports.h>



namespace ezbake { namespace thriftutils { namespace tasync {

class TAsioTransport : public ::apache::thrift::transport::TVirtualTransport<TAsioTransport,
                                                                             ::apache::thrift::transport::TBufferBase> {
public:
    //Async callback function type
    typedef ::boost::function<void(const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& )> Callback;


public:
    /*
     * Destructor
     */
    virtual ~TAsioTransport() {}


    /*
     * Write data from TMemoryBuffer to the asio stream
     *
     * stream   asio stream to write to
     * buffer   TMemoryBuffer holding data to write
     * cb       callback on completion or error
     */
    template <typename AsioStream>
    void writeFromBuffer(const ::boost::shared_ptr<AsioStream>& stream,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& buffer,
            const Callback& cb) {
        BOOST_THROW_EXCEPTION(::apache::thrift::transport::TTransportException("Cannot writeFromBuffer for base TAsioTransport."));
    }


    /*
     * Read from the asio stream to a TMemoryBuffer
     *
     * stream   asio stream to read from
     * buffer   TMemoryBuffer holding buffer to write read data to
     * cb       callback on completion or error
     */
    template <typename AsioStream>
    void readToBuffer(const ::boost::shared_ptr<AsioStream>& stream,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& buffer,
            const Callback& cb) {
        BOOST_THROW_EXCEPTION(::apache::thrift::transport::TTransportException("Cannot readToBuffer for base TAsioTransport."));
    }


    virtual void open() {
        BOOST_THROW_EXCEPTION(::apache::thrift::transport::TTransportException("Cannot open base TAsioTransport."));
    }

    virtual bool isOpen() {
        return true; //this is a managed async transport. We're always open
    }

    virtual bool peek() {
      return false; //this is a managed async transport. We're unable to peek
    }

    virtual void close() {
        BOOST_THROW_EXCEPTION(::apache::thrift::transport::TTransportException("Cannot close base TAsioTransport."));
    }

    virtual void writeSlow(const uint8_t* buf, uint32_t len) {
        //Do nothing for base TAsioTransport. Desired subclasses should implement this method
        (void) buf; (void) len; return;
    }

    virtual uint32_t readSlow(uint8_t* buf, uint32_t len) {
        //Do nothing for base TAsioTransport. Desired subclasses should implement this method
        (void) buf; (void) len; return 0;
    }

    virtual const uint8_t* borrowSlow(uint8_t* buf, uint32_t* len) {
        //Do nothing for base TAsioTransport. Desired subclasses should implement this method
        (void) buf; (void) len; return NULL;
    }

    virtual uint32_t readAll(uint8_t* buf, uint32_t len) {
        //Do nothing for base TAsioTransport. Desired subclasses should implement this method
        (void) buf; (void) len; return 0;
    }


protected:
    /*
     * Constructor
     *
     * strand   asio ioservice strand used to guarantee that none of the transport async
     * handlers will execute concurrently.
     */
    TAsioTransport(::boost::asio::io_service::strand& strand) : _strand(strand) {}


protected:
    ::boost::asio::io_service::strand& _strand;
};



class TAsioOutputTransport : public TAsioTransport {
public:
    /*
     * Constructor
     */
    TAsioOutputTransport(::boost::asio::io_service::strand& strand) :
        TAsioTransport(strand),
        _wBufSize(::apache::thrift::transport::TFramedTransport::DEFAULT_BUFFER_SIZE),
        _wBuf(new uint8_t[_wBufSize])
    {
        //initialize TVirtualTransport buffers
        this->setReadBuffer(NULL, 0);
        this->setWriteBuffer(_wBuf.get(), _wBufSize);

        // Pad the buffer so we can insert the frame size later.
        int32_t pad = 0;
        this->write((uint8_t*)&pad, sizeof(pad));
    }


    /*
     * Destructor
     */
    virtual ~TAsioOutputTransport() {}


    /*
     * Write data from TMemoryBuffer to the transport
     *
     * stream   asio stream to write to
     * buffer   TMemoryBuffer holding data to write
     * cb       callback on completion or error
     */
    template <typename AsioStream>
    void writeFromBuffer(const ::boost::shared_ptr<AsioStream>& stream,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& buffer,
            const Callback& cb) {

        //write the contents of the memory buffer transport to our framed transport
        uint8_t* rawBuffer;
        uint32_t rawLength;
        buffer->getBuffer(&rawBuffer, &rawLength);
        this->write(rawBuffer, rawLength);

        //flush our framed transport - send data off asynchronously
        flush(stream, cb);
    }

protected:
    virtual void writeSlow(const uint8_t* buf, uint32_t len) {
        /* using algorithm from thrift::transport::TFramedTransport */

        // Double buffer size until sufficient.
        uint32_t have = static_cast<uint32_t>(wBase_ - _wBuf.get());
        uint32_t new_size = _wBufSize;
        if (len + have < have /* overflow */ || len + have > 0x7fffffff) {
            BOOST_THROW_EXCEPTION(::apache::thrift::transport::TTransportException(::apache::thrift::transport::TTransportException::BAD_ARGS,
              "Attempted to write over 2 GB to TAsioOutputTransport."));
        }
        while (new_size < len + have) {
            new_size = new_size > 0 ? new_size * 2 : 1;
        }

        // Allocate new buffer.
        uint8_t* new_buf = new uint8_t[new_size];

        // Copy the old buffer to the new one.
        memcpy(new_buf, _wBuf.get(), have);

        // Now point buf to the new one.
        _wBuf.reset(new_buf);
        _wBufSize = new_size;
        wBase_ = _wBuf.get() + have;
        wBound_ = _wBuf.get() + _wBufSize;

        // Copy the data into the new buffer.
        memcpy(wBase_, buf, len);
        wBase_ += len;
    }


    /*
     * writes all buffered data to stream
     */
    template <typename AsioStream>
    void flush(const ::boost::shared_ptr<AsioStream>& stream, const Callback& cb) {
        int32_t sz_hbo = 0, sz_nbo = 0;
        assert(_wBufSize > sizeof(sz_nbo));

        // Slip the frame size into the start of the buffer which we padded earlier
        sz_hbo = static_cast<uint32_t>(wBase_ - (_wBuf.get() + sizeof(sz_nbo)));
        sz_nbo = (int32_t)htonl((uint32_t)(sz_hbo));
        memcpy(_wBuf.get(), (uint8_t*)&sz_nbo, sizeof(sz_nbo));

        if (sz_hbo > 0) {
            // Note that we reset wBase_ (with a pad for the frame size)
            // prior to the underlying write to ensure we're in a sane state
            // (i.e. internal buffer cleaned) if the underlying write fails
            wBase_ = _wBuf.get() + sizeof(sz_nbo);

            //Dispatch asynchronous call to Write size and frame body.
            ::boost::asio::async_write(*stream,
                    ::boost::asio::buffer(_wBuf.get(), static_cast<uint32_t>(sizeof(sz_nbo))+sz_hbo),
                    _strand.wrap(::boost::bind(&TAsioOutputTransport::handleFlushComplete, this,
                                 ::boost::asio::placeholders::error, cb)));
        }
    }

    /*
     * Handler for async write completion
     */
    void handleFlushComplete(const ::boost::system::error_code& error, const Callback& cb) {
        ::boost::shared_ptr< ::apache::thrift::transport::TTransportException> err;

        if (error) {
            err = ::boost::make_shared< ::apache::thrift::transport::TTransportException>(::apache::thrift::transport::TTransportException::INTERNAL_ERROR,
                    "Async write error: " + error.message());
        }

        /*
         * Done writing data or error detected.
         * Trigger root return callback.
         * Call callback for principal whom dispatched async write.
         * No other asynchronous calls to dispatch
         */
        cb(err);
    }


private:
    uint32_t _wBufSize;
    ::boost::scoped_array<uint8_t> _wBuf;
};



class TAsioInputTransport : public TAsioTransport {
public:
    /*
     * Constructor
     */
    TAsioInputTransport(::boost::asio::io_service::strand& strand) :
        TAsioTransport(strand)
    {
        this->setReadBuffer(NULL, 0);
        this->setWriteBuffer(NULL, 0);
    }

    /*
     * Destructor
     */
    virtual ~TAsioInputTransport() {}

    /*
     * Read from the asio stream to a TMemoryBuffer
     *
     * stream   asio stream to read from
     * buffer   TMemoryBuffer holding buffer to write read data to
     * cb       callback on completion or error
     */
    template <typename AsioStream>
    void readToBuffer(const ::boost::shared_ptr<AsioStream>& stream,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& buffer,
            const Callback& cb) {
        readFrame(stream, buffer, cb);
    }


protected:
    /*
     * Read frame from wire
     */
    template <typename AsioStream>
    void readFrame(const ::boost::shared_ptr<AsioStream>& stream,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& rtnBuffer,
            const Callback& cb) {
        ::boost::shared_ptr<int32_t> length = ::boost::make_shared<int32_t>(-1);

        //first read the frame length
        ::boost::asio::async_read(*stream,
                ::boost::asio::buffer(reinterpret_cast<uint8_t*>(length.get()), sizeof(int32_t)),
                _strand.wrap(::boost::bind(&TAsioInputTransport::handleReadFrameLength<AsioStream>,
                                           this,
                                           ::boost::asio::placeholders::error,
                                           ::boost::asio::placeholders::bytes_transferred,
                                           stream, length, rtnBuffer, cb)));
    }


    /*
     * Handler for reading the frame header
     */
    template <typename AsioStream>
    void handleReadFrameLength(const ::boost::system::error_code& error,
            std::size_t bytesRead, const ::boost::shared_ptr<AsioStream>& stream,
            const ::boost::shared_ptr<int32_t>& length,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& rtnBuffer,
            const Callback& cb) {
        ::boost::shared_ptr< ::apache::thrift::transport::TTransportException> err;
        int32_t frameLength = ntohl(*length);

        if (error) {
            err = ::boost::make_shared< ::apache::thrift::transport::TTransportException>(::apache::thrift::transport::TTransportException::INTERNAL_ERROR,
                    "Async read error: " + error.message());
        } else if (bytesRead < sizeof(int32_t)) {
            err = ::boost::make_shared< ::apache::thrift::transport::TTransportException>(::apache::thrift::transport::TTransportException::END_OF_FILE,
                    "Async read error: No more data to read after reading partial frame header.");
        } else if (frameLength < 0) {
            err = ::boost::make_shared< ::apache::thrift::transport::TTransportException>("Frame size has negative value.");
        }
        else {
            /*
             * No error.
             * Proceed to read the frame payload.
             */
            readFramePayload(stream, rtnBuffer, cb, frameLength);
            return;
        }

        //trigger principal return callback on error
        cb(err);
    }


    /*
     * Read payload from frame
     */
    template <typename AsioStream>
    void readFramePayload(const ::boost::shared_ptr<AsioStream>& stream,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& rtnBuffer,
            const Callback& cb, int32_t bytesToRead) {
        try {
            //Get pointer to where we can write the date to be read to.
            //TMemoryBuffer preallocates the required space if needed.
            uint8_t* rbuffer = rtnBuffer->getWritePtr(bytesToRead);

            ::boost::asio::async_read(*stream,
                    ::boost::asio::buffer(rbuffer, bytesToRead),
                    _strand.wrap(::boost::bind(&TAsioInputTransport::handleReadFramePayload,
                                               this,
                                               ::boost::asio::placeholders::error,
                                               ::boost::asio::placeholders::bytes_transferred,
                                               rtnBuffer, cb)));
        } catch (const ::std::exception& e) {
            //getWritePtr failed on return TMemoryBuffer. Inform principal whom dispatched async read
            rtnBuffer->resetBuffer();
            ::boost::shared_ptr< ::apache::thrift::transport::TTransportException> err =
                    ::boost::make_shared< ::apache::thrift::transport::TTransportException>(::apache::thrift::transport::TTransportException::CORRUPTED_DATA,
                            "ERROR! Unable to read payload (Exception in TMemoryBuffer): " + boost::diagnostic_information(e));
            cb(err);
        }
    }


    /*
     * Handler for reading frame payload
     */
    void handleReadFramePayload(const ::boost::system::error_code& error, std::size_t bytesRead,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& rtnBuffer,
            const Callback& cb) {
        ::boost::shared_ptr< ::apache::thrift::transport::TTransportException> err;

        if (error) {
            err = ::boost::make_shared< ::apache::thrift::transport::TTransportException>(::apache::thrift::transport::TTransportException::INTERNAL_ERROR,
                    "Async read error: " + error.message());
        } else {
            try {
                //No error, update our read TMemoryBuffer transport to indicated the amount of data actually read
                rtnBuffer->wroteBytes(static_cast<uint32_t>(bytesRead));
            } catch (const ::std::exception& e) {
                //wroteBytes failed on return TMemoryBuffer. Inform principal whom dispatched async read
                rtnBuffer->resetBuffer();
                err = ::boost::make_shared< ::apache::thrift::transport::TTransportException>(::apache::thrift::transport::TTransportException::CORRUPTED_DATA,
                        "ERROR! Unable to read payload (Exception in TMemoryBuffer): " + boost::diagnostic_information(e));
            }
        }

        /*
         * Done reading data or error detected.
         * Trigger root return callback.
         * Call callback for principal whom dispatched async read.
         * No other asynchronous calls to dispatch
         */
        cb(err);
    }
};

}}} // namespace ::ezbakle::thriftutils::tasync

#endif /* EZBAKE_THRIFTUTILS_TASYNC_TASYNCASIOTRANSPORT_H_ */
