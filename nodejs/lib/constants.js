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

module.exports = {
  SSL: {
    SSL_DIR_KEY: "ezbake.security.ssl.dir",
    PRIVATE_KEY_FILE: "ezbake.ssl.privatekey.file",
    DEFAULT_PRIVATE_KEY_FILE: "application.priv",
    PUBLIC_KEY_FILE: "ezbake.ssl.servicekey.file",
    DEFAULT_PUBLIC_KEY_FILE: "application.pub",
    CERT_FILE: "ezbake.ssl.certificate.file",
    DEFAULT_CERT_FILE: "application.crt",
    DEFAULT_CA_FILE: "ezbakeca.crt"
  },
  CHAR_SET: "utf8"
}