// Item type codes
const U8   = 0x01;
const I8   = 0x11;
const U16  = 0x02;
const I16  = 0x12;
const U32  = 0x04;
const I32  = 0x14;
const U64  = 0x08;
const I64  = 0x18;
const SZ   = 0x21;
const BLOB = 0x41;
const BLOB_DATA = 0x42;
const BLOB_IDX = 0x48;

// Few Page constants
const HEADER_SIZE = 32;
const BITMAPARRAY_OFFSET = 32;
const BITMAPARRAY_SIZE_IN_BYTES = 32;
const FIRST_ENTRY_OFFSET = 64;
const SINGLE_ENTRY_SIZE = 32;
const CHUNK_ANY = 0xFF;
const ACTIVE = 0xFFFFFFFE;
const FULL = 0xFFFFFFFC;
const VERSION2 = 0xFE;


async function generate(params) {
    /*
    Generate NVS Partition
    */

    let input_size = checkSize(params.size);

    const input_file = makeFileIterator(await getTemplateFile(params.input));
    let binaryOutput = {data: []};
    let nvs_obj = nvsOpen(binaryOutput, input_size);
    logMsg('Creating NVS binary');

    let line = input_file.next();

    // Comments are skipped
    while(line.value.startsWith('#')) {
        line = input_file.next();
    }

    header = line.value.split(',');

    while(true) {
        line = input_file.next();

        let value = line.value.split(',');
        if (value.length == 1 && value.includes('')) {
            break;
        }
        data = Object.fromEntries(zipLongest(header, value));
        if (data.key in params.data) {
            data.value = params.data[data.key];
        }
        try {
            // Check key length
            if (data.key.length > 15) {
                throw('Length of key ' + data.key + ' should be <= 15 characters.');
            }
            writeEntry(nvs_obj, data.key, data.type, data.encoding, data.value);
        } catch(e) {
            throw(e)
        }
    }
    nvsClose(nvs_obj);

    return binaryOutput.data;
}

function nvsOpen(result_obj, input_size) {
    /* Wrapper to create and NVS class object. This object can later be used to set key-value pairs

    :param result_obj: File/Stream object to dump resultant binary. If data is to be dumped into memory, one way is to use BytesIO object
    :param input_size: Size of Partition
    :return: NVS class instance
    */
    return new NVS(result_obj, input_size);
}

function writeEntry(nvs_instance, key, datatype, encoding, value) {
    /* Wrapper to set key-value pair in NVS format

    :param nvs_instance: Instance of an NVS class returned by nvsOpen()
    :param key: Key of the data
    :param datatype: Data type. Valid values are "file", "data" and "namespace"
    :param encoding: Data encoding. Valid values are "u8", "i8", "u16", "i16", "u32", "i32", "u64", "i64", "string", "binary", "hex2bin" and "base64"
    :param value: Data value in ascii encoded string format for "data" datatype and filepath for "file" datatype
    :return: None
    */

    if (datatype == 'file') {
        throw("Files are not supported");
    }

    if (datatype == 'namespace') {
        nvs_instance.writeNamespace(key);
    } else {
        nvs_instance.writeEntry(key, value, encoding);
    }
}

function nvsClose(nvs_instance) {
    /* Wrapper to finish writing to NVS and write data to file/stream object provided to nvsOpen method

    :param nvs_instance: Instance of NVS class returned by nvsOpen()
    :return: None
    */
    nvs_instance.finish();
}

function checkSize(size) {
    /*
    Checks for input partition size
    :param size: Input partition size
    */
    try {
        // Set size
        let input_size = parseInt(size)
        if (input_size % 4096 != 0) {
          throw('Size of partition must be multiple of 4096');
        }

        // Update size as a page needs to be reserved of size 4KB
        input_size = input_size - Page.PAGE_PARAMS['max_size']

        if (input_size < (2 * Page.PAGE_PARAMS['max_size'])) {
          throw('Minimum NVS partition size needed is 0x3000 bytes.')
        }

        return input_size
    } catch(e) {
        errorMsg(e);
        return;
    }
}

class Page {
  static PAGE_PARAMS = {
        'max_size': 4096,
        'max_old_blob_size': 1984,
        'max_new_blob_size': 4000,
        'max_entries': 126
    }

    constructor(page_num, isRsrvPage) {
        this.entry_num = 0;
        this.bitmap_array = [];
        this.page_buf = new Array(Page.PAGE_PARAMS['max_size']).fill(0xFF);
        if (!isRsrvPage) {
            this.bitmap_array = this.createBitmapArray();
            this.setHeader(page_num);
        }
    }

    setHeader(page_num) {
        // set page state to active
        let page_header = new Array(32).fill(0xFF);
        page_header = page_header.replaceAt(0, struct.pack('<I', ACTIVE));
        // set page sequence number
        page_header = page_header.replaceAt(4, struct.pack('<I', page_num));
        // set version
        page_header[8] = VERSION2;
        // set header's CRC
        let crc_data = page_header.slice(4, 28);
        let crc = crc32(crc_data, 0xFFFFFFFF);
        page_header = page_header.replaceAt(28, struct.pack('<I', crc & 0xFFFFFFFF));
        this.page_buf = this.page_buf.replaceAt(0, page_header);
    }

    createBitmapArray() {
        return new Array(32).fill(0xFF);
    }

    writeBitmapArray() {
        let bitnum = this.entry_num * 2;
        let byte_idx = parseInt(bitnum / 8);  // Find byte index in the array
        let bit_offset = bitnum & 7;  // Find bit offset in given byte index
        let mask = ~(1 << bit_offset);
        this.bitmap_array[byte_idx] &= mask;
        this.page_buf = this.page_buf.replaceAt(BITMAPARRAY_OFFSET, this.bitmap_array);
    }

    writeEntryToBuf(data, entrycount, nvs_obj) {
        let encr_data = [];

        let data_offset = FIRST_ENTRY_OFFSET + (SINGLE_ENTRY_SIZE * this.entry_num);
        this.page_buf = this.page_buf.replaceAt(data_offset, data);

        // Set bitmap array for entries in current page
        for (let i = 0; i < entrycount; i++) {
            this.writeBitmapArray();
            this.entry_num += 1;
        }
    }

    setCrcHeader(entry_struct) {
        let crc_data = toByteArray('28');
        crc_data = crc_data.replaceAt(0, entry_struct.slice(0, 4));
        crc_data = crc_data.replaceAt(4, entry_struct.slice(8, 32));
        let crc = crc32(crc_data, 0xFFFFFFFF);
        entry_struct = entry_struct.replaceAt(4, struct.pack('<I', crc & 0xFFFFFFFF));
        return entry_struct;
    }

    writeVarLenBinaryData(entry_struct, ns_index, key, data, data_size, total_entry_count, encoding, nvs_obj) {
        chunk_start = 0
        chunk_count = 0
        chunk_index = CHUNK_ANY
        offset = 0
        remaining_size = data_size
        let tailroom = null;

        while(true) {
            chunk_size = 0

            // Get the size available in current page
            tailroom = (Page.PAGE_PARAMS['max_entries'] - this.entry_num - 1) * SINGLE_ENTRY_SIZE;
            if (tailroom < 0) {
              throw('Page overflow!!');
            }

            // Split the binary data into two and store a chunk of available size onto curr page
            if (tailroom < remaining_size) {
                chunk_size = tailroom;
            } else {
                chunk_size = remaining_size;
            }

            remaining_size = remaining_size - chunk_size;

            // Change type of data to BLOB_DATA
            entry_struct[1] = BLOB_DATA;

            // Calculate no. of entries data chunk will require
            datachunk_rounded_size = (chunk_size + 31) & ~31
            datachunk_entry_count = parseInt(datachunk_rounded_size / 32)
            datachunk_total_entry_count = datachunk_entry_count + 1  // +1 for the entry header

            // Set Span
            entry_struct[2] = datachunk_total_entry_count

            // Update the chunkIndex
            chunk_index = chunk_start + chunk_count
            entry_struct[3] = chunk_index

            // Set data chunk
            data_chunk = data.slice(offset, offset + chunk_size)

            // Compute CRC of data chunk
            entry_struct = entry_struct.replaceAt(24, struct.pack('<H', chunk_size));

            if (!(data instanceof Array)) {
                data_chunk = toByteArray(data_chunk);
            }

            crc = crc32(data_chunk, 0xFFFFFFFF);
            entry_struct = entry_struct.replaceAt(28, struct.pack('<I', crc & 0xFFFFFFFF));

            // compute crc of entry header
            entry_struct = this.setCrcHeader(entry_struct)

            // write entry header
            this.writeEntryToBuf(entry_struct, 1, nvs_obj)
            // write actual data
            this.writeEntryToBuf(data_chunk, datachunk_entry_count, nvs_obj)

            chunk_count = chunk_count + 1

            if (remaining_size || (tailroom - chunk_size) < SINGLE_ENTRY_SIZE) {
                nvs_obj.createNewPage();
                //this = nvs_obj.cur_page;
            }

            offset = offset + chunk_size

            // All chunks are stored, now store the index
            if (!remaining_size) {
                // Initialise data field to 0xff
                data_array = new Array(8).fill(0xFF);
                entry_struct = entry_struct.replaceAt(24, data_array);

                // change type of data to BLOB_IDX
                entry_struct[1] = BLOB_IDX;

                // Set Span
                entry_struct[2] = 1;

                // Update the chunkIndex
                chunk_index = CHUNK_ANY;
                entry_struct[3] = chunk_index;

                entry_struct = entry_struct.replaceAt(24, struct.pack('<I', data_size));
                entry_struct[28] = chunk_count;
                entry_struct[29] = chunk_start;

                // compute crc of entry header
                entry_struct = this.setCrcHeader(entry_struct)

                // write last entry
                this.writeEntryToBuf(entry_struct, 1, nvs_obj)
                break
            }
        }
        return entry_struct
    }

    writeSinglePageEntry(entry_struct, data, datalen, data_entry_count, nvs_obj) {
        // compute CRC of data
        entry_struct = entry_struct.replaceAt(24, struct.pack('<H', datalen));

        if (!(data instanceof Array)) {
            data = toByteArray(data);
        }

        let crc = crc32(data, 0xFFFFFFFF);
        entry_struct = entry_struct.replaceAt(28, struct.pack('<I', crc & 0xFFFFFFFF));

        // compute crc of entry header
        entry_struct = this.setCrcHeader(entry_struct);

        // write entry header
        this.writeEntryToBuf(entry_struct, 1, nvs_obj);
        // write actual data
        this.writeEntryToBuf(data, data_entry_count, nvs_obj);
    }

    /*
    Low-level function to write variable length data into page buffer. Data should be formatted
    according to encoding specified.
    */
    writeVarLenData(key, data, encoding, ns_index, nvs_obj) {
        // Set size of data
        let datalen = data.length;

        if (datalen > Page.PAGE_PARAMS['max_old_blob_size']) {
            if (encoding == 'string') {
                throw new InputError(' Input File: Size (' + datalen + ') exceeds max allowed length `' +
                                 Page.PAGE_PARAMS['max_old_blob_size'] + '` bytes for key `' + key + '`.')
            }
        }

        // Calculate no. of entries data will require
        let rounded_size = (datalen + 31) & ~31;
        let data_entry_count = parseInt(rounded_size / 32);
        let total_entry_count = data_entry_count + 1;  // +1 for the entry header

        // Check if page is already full and new page is needed to be created right away
        if (this.entry_num >= Page.PAGE_PARAMS['max_entries']) {
            throw new PageFullError();
        } else if ((this.entry_num + total_entry_count) >= Page.PAGE_PARAMS['max_entries']) {
            if (!(['hex2bin', 'binary', 'base64'].includes(encoding))) {
                throw new PageFullError()
            }
        }

        // Entry header
        let entry_struct = new Array(32).fill(0xFF);
        // Set Namespace Index
        entry_struct[0] = ns_index;
        // Set Span
        if (encoding == 'string') {
            entry_struct[2] = data_entry_count + 1;
        }
        // Set Chunk Index
        entry_struct[3] = CHUNK_ANY;

        // set key
        let key_array = new Array(16).fill(0x00);
        entry_struct = entry_struct.replaceAt(8, key_array);
        entry_struct = entry_struct.replaceAt(8, toByteArray(key));

        // set Type
        if (encoding == 'string') {
            entry_struct[1] = SZ;
        } else if (encoding in ['hex2bin', 'binary', 'base64']) {
            entry_struct[1] = BLOB;
        }

        if (['hex2bin', 'binary', 'base64'].includes(encoding)) {
            entry_struct = this.writeVarLenBinaryData(entry_struct, ns_index, key,data,
                                                         datalen, total_entry_count, encoding, nvs_obj)
        } else {
            this.writeSinglePageEntry(entry_struct, data, datalen, data_entry_count, nvs_obj)
        }
    }

    /* Low-level function to write data of primitive type into page buffer. */
    writePrimitiveData(key, data, encoding, ns_index,nvs_obj) {
        // Check if entry exceeds max number of entries allowed per page
        if (this.entry_num >= Page.PAGE_PARAMS['max_entries']) {
            throw new PageFullError();
        }

        let entry_struct = new Array(32).fill(0xFF);
        entry_struct[0] = ns_index;  // namespace index
        entry_struct[2] = 0x01;  // Span
        entry_struct[3] = CHUNK_ANY;

        // write key
        let key_array = new Array(16).fill(0x00);

        entry_struct = entry_struct.replaceAt(8, key_array);
        entry_struct = entry_struct.replaceAt(8, toByteArray(key));

        if (encoding == 'u8') {
            entry_struct[1] = U8;
            entry_struct = entry_struct.replaceAt(24, struct.pack('<B', data));
        } else if (encoding == 'u16') {
            entry_struct[1] = U16;
            entry_struct = entry_struct.replaceAt(24, struct.pack('<H', data));
        } else if (encoding == 'u32') {
            entry_struct[1] = U32;
            entry_struct = entry_struct.replaceAt(24, struct.pack('<I', data));
        } else if (encoding == 'u64') {
            entry_struct[1] = U64;
            entry_struct = entry_struct.replaceAt(24, struct.pack('<Q', data));
        }

        // Compute CRC
        let crc_data = toByteArray('28');
        crc_data = crc_data.replaceAt(0, entry_struct.slice(0, 4));
        crc_data = crc_data.replaceAt(4, entry_struct.slice(8, 32));
        let crc = crc32(crc_data, 0xFFFFFFFF)
        entry_struct = entry_struct.replaceAt(4, struct.pack('<I', crc & 0xFFFFFFFF));

        // write to file
        this.writeEntryToBuf(entry_struct, 1, nvs_obj);
    }

    /* Get page buffer data of a given page */
    get_data() {
        return this.page_buf;
    }
}

class NVS {
    constructor(fout, input_size) {
        this.size = input_size;
        this.namespace_idx = 0
        this.page_num = -1
        this.pages = []
        this.fout = fout
        this.cur_page = this.createNewPage();
    }

    finish() {
        while(true) {
            try {
                this.createNewPage();
            } catch(e) {
                if (e instanceof InsufficientSizeError) {
                    this.size = null;
                    // Creating the last reserved page
                    this.createNewPage(true);
                    break;
                }
            }
        }
        let result = this.getBinaryData();
        this.fout.data = this.fout.data.concat(result);
    }

    createNewPage(isRsrvPage=false) {
        // Set previous page state to FULL before creating new page
        if (this.pages.length) {
            let curr_page_state = struct.unpack('<I', this.cur_page.page_buf.slice(0, 4));
            if (curr_page_state == ACTIVE) {
                this.cur_page.page_buf = this.cur_page.page_buf.replaceAt(0, struct.pack('<I', FULL));
            }
        }
        // Update available size as each page is created
        if (this.size == 0) {
            throw new InsufficientSizeError('Error: Size parameter is less than the size of data in csv. Please increase size.');
        }
        if (!isRsrvPage) {
            this.size = this.size - Page.PAGE_PARAMS['max_size'];
        }
        this.page_num += 1;
        // Set version for each page and page header
        let new_page = new Page(this.page_num, isRsrvPage);
        this.pages.push(new_page);
        this.cur_page = new_page;
        return new_page;
    }

    /*
    Write namespace entry and subsequently increase namespace count so that all upcoming entries
    will be mapped to a new namespace.
    */
    writeNamespace(key) {
        this.namespace_idx += 1;
        try {
            this.cur_page.writePrimitiveData(key, this.namespace_idx, 'u8', 0, this);
        } catch(e) {
            if (e instanceof PageFullError) {
                new_page = this.createNewPage();
                new_page.writePrimitiveData(key, this.namespace_idx, 'u8', 0, this);
            }
        }
    }

    /*
    Write key-value pair. Function accepts value in the form of ascii character and converts
    it into appropriate format before calling Page class's functions to write entry into NVS format.
    Function handles PageFullError and creates a new page and re-invokes the function on a new page.
    We don't have to guard re-invocation with try-except since no entry can span multiple pages.
    */
    writeEntry(key, value, encoding) {
        if (encoding == 'hex2bin') {
            value = value.trim()
            if (value.length % 2 != 0) {
                throw new InputError('%s: Invalid data length. Should be multiple of 2.' % key);
            }
            value = binascii.a2b_hex(value);
        }

        if (encoding == 'base64') {
            value = binascii.a2b_base64(value);
        }

        if (encoding == 'string') {
            if (value instanceof Array) {
                value = fromByteArray(value);
            }
            value += '\0';
        }

        encoding = encoding.toLowerCase()
        let varlen_encodings = ['string', 'binary', 'hex2bin', 'base64'];
        let primitive_encodings = ['u8', 'i8', 'u16', 'i16', 'u32', 'i32', 'u64', 'i64'];

        if (varlen_encodings.includes(encoding)) {
            try {
                this.cur_page.writeVarLenData(key, value, encoding, this.namespace_idx, this);
            } catch(e) { // PageFullError
                let new_page = this.createNewPage();
                new_page.writeVarLenData(key, value, encoding, this.namespace_idx, this);
            }
        } else if (primitive_encodings.includes(encoding)) {
            try {
                this.cur_page.writePrimitiveData(key, parseInt(value), encoding, this.namespace_idx, this);
            } catch(e) {
                if (e instanceof PageFullError) {
                    let new_page = this.createNewPage();
                    new_page.writePrimitiveData(key, parseInt(value), encoding, this.namespace_idx, this);
                }
            }
        } else {
            throw(encoding + ': Unsupported encoding');
        }
    }

    /* Return accumulated data of all pages */
    getBinaryData() {
        data = [];
        for (let page of this.pages) {
            data = data.concat(page.get_data());
        }
        return data;
    }
}

/*
Represents error when current page doesn't have sufficient entries left
to accommodate current request
*/
class PageFullError extends Error {
    constructor(message) {
        super(message);
        this.name = "PageFullError";
    }
}

/*
Represents error on the input
*/
class InputError extends Error {
    constructor(message) {
        super(message);
        this.name = "InputError";
    }
}

/*
Represents error when NVS Partition size given is insufficient
to accomodate the data in the given csv file
*/
class InsufficientSizeError extends Error {
    constructor(message) {
        super(message);
        this.name = "InsufficientSizeError";
    }
}

async function getTemplateFile(templateFile) {
  let response = await fetch("/" + templateFile);
  let templateCsv = await response.text();
  return templateCsv;
}
