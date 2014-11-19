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
 * TAsyncClientManager.h
 *
 *  Created on: Apr 17, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_TASYNC_TASYNCCLIENTMANAGER_H_
#define EZBAKE_THRIFTUTILS_TASYNC_TASYNCCLIENTMANAGER_H_

#include <boost/ref.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/thread/thread.hpp>


namespace ezbake { namespace thriftutils { namespace tasync {

class TAsyncClientManager {
public:
    static const unsigned int DEFAULT_THREADS = 1;

public:
    /**
     * Construct  a new Client Manager specifying the number of threads to
     * use in the manager's thread pool.
     *
     * To support using multiple threads to handle the io service, event handlers need to be dispatched with
     * strands (::boost::asio::io_service::strand) to ensure strict sequential invocation of the event handlers
     * within an Async Channel.
     */
    TAsyncClientManager(unsigned int numThreads = DEFAULT_THREADS) :
        _work(::boost::make_shared< ::boost::asio::io_service::work>(::boost::ref(_ioService))) {
        for (unsigned int i = 0; i < numThreads; i++) {
            _threadpool.create_thread(::boost::bind(&::boost::asio::io_service::run, &_ioService));
        }
    }

    /*
     * Destructor
     */
    virtual ~TAsyncClientManager() {
        stop();
    }

    /**
     * Return a reference to the underlying io_service
     */
    ::boost::asio::io_service& getIoService() {
        return _work->get_io_service();
    }

    /**
     * Graceful shutdown.
     * Allow all unfinished operations and handlers to finish normally
     */
    void shutdown() {
        _work.reset();
        _threadpool.join_all();
    }

    /**
     * Stop as soon as possible abandoning unfinished operations
     */
    void stop() {
        _ioService.stop();
        _threadpool.join_all();
    }

private:
    ::boost::asio::io_service _ioService;
    ::boost::shared_ptr< ::boost::asio::io_service::work> _work;
    ::boost::thread_group _threadpool;
};

}}} // namespace ::ezbake::thriftutils::tasync

#endif /* EZBAKE_THRIFTUTILS_TASYNC_TASYNCCLIENTMANAGER_H_ */
