use alloc::{
    collections::{BTreeMap, LinkedList},
    vec::Vec,
};
use core::convert::TryFrom;

use crate::{
    error::{
        IncompatibleOptionValueFormat, InvalidContentFormat, InvalidObserve,
        MessageError,
    },
    header::{Header, HeaderRaw, MessageClass},
    option_value::{OptionValueType, OptionValueU16, OptionValueU32},
};

macro_rules! u8_to_unsigned_be {
    ($src:ident, $start:expr, $end:expr, $t:ty) => ({
        (0..=$end - $start).rev().fold(
            0, |acc, i| acc | $src[$start+i] as $t << i * 8
        )
    })
}

/// The CoAP options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CoapOption {
    IfMatch,
    UriHost,
    ETag,
    IfNoneMatch,
    Observe,
    UriPort,
    LocationPath,
    Oscore,
    UriPath,
    ContentFormat,
    MaxAge,
    UriQuery,
    Accept,
    /// Q-Block1 option (RFC 9177), number 19. Same value layout as Block1.
    QBlock1,
    LocationQuery,
    Block2,
    Block1,
    /// Q-Block2 option (RFC 9177), number 31. Same value layout as Block2.
    QBlock2,
    ProxyUri,
    ProxyScheme,
    Size1,
    Size2,
    NoResponse,
    Unknown(u16),
}

impl From<u16> for CoapOption {
    fn from(number: u16) -> CoapOption {
        match number {
            1 => CoapOption::IfMatch,
            3 => CoapOption::UriHost,
            4 => CoapOption::ETag,
            5 => CoapOption::IfNoneMatch,
            6 => CoapOption::Observe,
            7 => CoapOption::UriPort,
            8 => CoapOption::LocationPath,
            9 => CoapOption::Oscore,
            11 => CoapOption::UriPath,
            12 => CoapOption::ContentFormat,
            14 => CoapOption::MaxAge,
            15 => CoapOption::UriQuery,
            17 => CoapOption::Accept,
            19 => CoapOption::QBlock1,
            20 => CoapOption::LocationQuery,
            23 => CoapOption::Block2,
            27 => CoapOption::Block1,
            31 => CoapOption::QBlock2,
            35 => CoapOption::ProxyUri,
            39 => CoapOption::ProxyScheme,
            60 => CoapOption::Size1,
            28 => CoapOption::Size2,
            258 => CoapOption::NoResponse,
            _ => CoapOption::Unknown(number),
        }
    }
}

impl From<CoapOption> for u16 {
    fn from(option: CoapOption) -> u16 {
        match option {
            CoapOption::IfMatch => 1,
            CoapOption::UriHost => 3,
            CoapOption::ETag => 4,
            CoapOption::IfNoneMatch => 5,
            CoapOption::Observe => 6,
            CoapOption::UriPort => 7,
            CoapOption::LocationPath => 8,
            CoapOption::Oscore => 9,
            CoapOption::UriPath => 11,
            CoapOption::ContentFormat => 12,
            CoapOption::MaxAge => 14,
            CoapOption::UriQuery => 15,
            CoapOption::Accept => 17,
            CoapOption::QBlock1 => 19,
            CoapOption::LocationQuery => 20,
            CoapOption::Block2 => 23,
            CoapOption::Block1 => 27,
            CoapOption::QBlock2 => 31,
            CoapOption::ProxyUri => 35,
            CoapOption::ProxyScheme => 39,
            CoapOption::Size1 => 60,
            CoapOption::Size2 => 28,
            CoapOption::NoResponse => 258,
            CoapOption::Unknown(number) => number,
        }
    }
}

/// The content formats.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum ContentFormat {
    TextPlain,
    /// Media-Type: `application/cose; cose-type="cose-encrypt0"`, ID: 16
    ApplicationCoseEncrypt0,
    /// Media-Type: `application/cose; cose-type="cose-mac0"`, ID: 17
    ApplicationCoseMac0,
    /// Media-Type: `application/cose; cose-type="cose-sign1"`, ID: 18
    ApplicationCoseSign1,
    ApplicationAceCbor,
    ImageGif,
    ImageJpeg,
    ImagePng,
    ApplicationLinkFormat,
    ApplicationXML,
    ApplicationOctetStream,
    ApplicationEXI,
    ApplicationJSON,
    ApplicationJsonPatchJson,
    ApplicationMergePatchJson,
    ApplicationCBOR,
    ApplicationCWt,
    ApplicationMultipartCore,
    ApplicationCborSeq,
    /// Media-Type: `application/cose; cose-type="cose-encrypt"`, ID: 96
    ApplicationCoseEncrypt,
    /// Media-Type: `application/cose; cose-type="cose-mac"`, ID: 97
    ApplicationCoseMac,
    /// Media-Type: `application/cose; cose-type="cose-sign"`, ID: 98
    ApplicationCoseSign,
    ApplicationCoseKey,
    ApplicationCoseKeySet,
    ApplicationSenmlJSON,
    ApplicationSensmlJSON,
    ApplicationSenmlCBOR,
    ApplicationSensmlCBOR,
    ApplicationSenmlExi,
    ApplicationSensmlExi,
    /// Media-Type: `application/yang-data+cbor; id=sid`, ID: 140
    ApplicationYangDataCborSid,
    ApplicationCoapGroupJson,
    ApplicationDotsCbor,
    ApplicationMissingBlocksCborSeq,
    /// Media-Type: `application/pkcs7-mime; smime-type=server-generated-key`, ID: 280
    ApplicationPkcs7MimeServerGeneratedKey,
    /// Media-Type: `application/pkcs7-mime; smime-type=certs-only`, ID: 281
    ApplicationPkcs7MimeCertsOnly,
    ApplicationPkcs8,
    ApplicationCsrattrs,
    ApplicationPkcs10,
    ApplicationPkixCert,
    ApplicationAifCbor,
    ApplicationAifJson,
    ApplicationSenmlXML,
    ApplicationSensmlXML,
    ApplicationSenmlEtchJson,
    ApplicationSenmlEtchCbor,
    ApplicationYangDataCbor,
    /// Media-Type: `application/yang-data+cbor; id=name`, ID: 341
    ApplicationYangDataCborName,
    ApplicationTdJson,
    ApplicationVoucherCoseCbor,
    ApplicationVndOcfCbor,
    ApplicationOscore,
    ApplicationJavascript,
    ApplicationJsonDeflate,
    ApplicationCborDeflate,
    ApplicationVndOmaLwm2mTlv,
    ApplicationVndOmaLwm2mJson,
    ApplicationVndOmaLwm2mCbor,
    TextCss,
    ImageSvgXml,
}

impl TryFrom<usize> for ContentFormat {
    type Error = InvalidContentFormat;

    fn try_from(number: usize) -> Result<ContentFormat, InvalidContentFormat> {
        match number {
            0 => Ok(ContentFormat::TextPlain),
            16 => Ok(ContentFormat::ApplicationCoseEncrypt0),
            17 => Ok(ContentFormat::ApplicationCoseMac0),
            18 => Ok(ContentFormat::ApplicationCoseSign1),
            19 => Ok(ContentFormat::ApplicationAceCbor),
            21 => Ok(ContentFormat::ImageGif),
            22 => Ok(ContentFormat::ImageJpeg),
            23 => Ok(ContentFormat::ImagePng),
            40 => Ok(ContentFormat::ApplicationLinkFormat),
            41 => Ok(ContentFormat::ApplicationXML),
            42 => Ok(ContentFormat::ApplicationOctetStream),
            47 => Ok(ContentFormat::ApplicationEXI),
            50 => Ok(ContentFormat::ApplicationJSON),
            51 => Ok(ContentFormat::ApplicationJsonPatchJson),
            52 => Ok(ContentFormat::ApplicationMergePatchJson),
            60 => Ok(ContentFormat::ApplicationCBOR),
            61 => Ok(ContentFormat::ApplicationCWt),
            62 => Ok(ContentFormat::ApplicationMultipartCore),
            63 => Ok(ContentFormat::ApplicationCborSeq),
            96 => Ok(ContentFormat::ApplicationCoseEncrypt),
            97 => Ok(ContentFormat::ApplicationCoseMac),
            98 => Ok(ContentFormat::ApplicationCoseSign),
            101 => Ok(ContentFormat::ApplicationCoseKey),
            102 => Ok(ContentFormat::ApplicationCoseKeySet),
            110 => Ok(ContentFormat::ApplicationSenmlJSON),
            111 => Ok(ContentFormat::ApplicationSensmlJSON),
            112 => Ok(ContentFormat::ApplicationSenmlCBOR),
            113 => Ok(ContentFormat::ApplicationSensmlCBOR),
            114 => Ok(ContentFormat::ApplicationSenmlExi),
            115 => Ok(ContentFormat::ApplicationSensmlExi),
            140 => Ok(ContentFormat::ApplicationYangDataCborSid),
            256 => Ok(ContentFormat::ApplicationCoapGroupJson),
            271 => Ok(ContentFormat::ApplicationDotsCbor),
            272 => Ok(ContentFormat::ApplicationMissingBlocksCborSeq),
            280 => Ok(ContentFormat::ApplicationPkcs7MimeServerGeneratedKey),
            281 => Ok(ContentFormat::ApplicationPkcs7MimeCertsOnly),
            284 => Ok(ContentFormat::ApplicationPkcs8),
            285 => Ok(ContentFormat::ApplicationCsrattrs),
            286 => Ok(ContentFormat::ApplicationPkcs10),
            287 => Ok(ContentFormat::ApplicationPkixCert),
            290 => Ok(ContentFormat::ApplicationAifCbor),
            291 => Ok(ContentFormat::ApplicationAifJson),
            310 => Ok(ContentFormat::ApplicationSenmlXML),
            311 => Ok(ContentFormat::ApplicationSensmlXML),
            320 => Ok(ContentFormat::ApplicationSenmlEtchJson),
            322 => Ok(ContentFormat::ApplicationSenmlEtchCbor),
            340 => Ok(ContentFormat::ApplicationYangDataCbor),
            341 => Ok(ContentFormat::ApplicationYangDataCborName),
            432 => Ok(ContentFormat::ApplicationTdJson),
            836 => Ok(ContentFormat::ApplicationVoucherCoseCbor),
            10000 => Ok(ContentFormat::ApplicationVndOcfCbor),
            10001 => Ok(ContentFormat::ApplicationOscore),
            10002 => Ok(ContentFormat::ApplicationJavascript),
            11050 => Ok(ContentFormat::ApplicationJsonDeflate),
            11060 => Ok(ContentFormat::ApplicationCborDeflate),
            11542 => Ok(ContentFormat::ApplicationVndOmaLwm2mTlv),
            11543 => Ok(ContentFormat::ApplicationVndOmaLwm2mJson),
            11544 => Ok(ContentFormat::ApplicationVndOmaLwm2mCbor),
            20000 => Ok(ContentFormat::TextCss),
            30000 => Ok(ContentFormat::ImageSvgXml),
            _ => Err(InvalidContentFormat),
        }
    }
}

impl From<ContentFormat> for usize {
    fn from(format: ContentFormat) -> usize {
        match format {
            ContentFormat::TextPlain => 0,
            ContentFormat::ApplicationCoseEncrypt0 => 16,
            ContentFormat::ApplicationCoseMac0 => 17,
            ContentFormat::ApplicationCoseSign1 => 18,
            ContentFormat::ApplicationAceCbor => 19,
            ContentFormat::ImageGif => 21,
            ContentFormat::ImageJpeg => 22,
            ContentFormat::ImagePng => 23,
            ContentFormat::ApplicationLinkFormat => 40,
            ContentFormat::ApplicationXML => 41,
            ContentFormat::ApplicationOctetStream => 42,
            ContentFormat::ApplicationEXI => 47,
            ContentFormat::ApplicationJSON => 50,
            ContentFormat::ApplicationJsonPatchJson => 51,
            ContentFormat::ApplicationMergePatchJson => 52,
            ContentFormat::ApplicationCBOR => 60,
            ContentFormat::ApplicationCWt => 61,
            ContentFormat::ApplicationMultipartCore => 62,
            ContentFormat::ApplicationCborSeq => 63,
            ContentFormat::ApplicationCoseEncrypt => 96,
            ContentFormat::ApplicationCoseMac => 97,
            ContentFormat::ApplicationCoseSign => 98,
            ContentFormat::ApplicationCoseKey => 101,
            ContentFormat::ApplicationCoseKeySet => 102,
            ContentFormat::ApplicationSenmlJSON => 110,
            ContentFormat::ApplicationSensmlJSON => 111,
            ContentFormat::ApplicationSenmlCBOR => 112,
            ContentFormat::ApplicationSensmlCBOR => 113,
            ContentFormat::ApplicationSenmlExi => 114,
            ContentFormat::ApplicationSensmlExi => 115,
            ContentFormat::ApplicationYangDataCborSid => 140,
            ContentFormat::ApplicationCoapGroupJson => 256,
            ContentFormat::ApplicationDotsCbor => 271,
            ContentFormat::ApplicationMissingBlocksCborSeq => 272,
            ContentFormat::ApplicationPkcs7MimeServerGeneratedKey => 280,
            ContentFormat::ApplicationPkcs7MimeCertsOnly => 281,
            ContentFormat::ApplicationPkcs8 => 284,
            ContentFormat::ApplicationCsrattrs => 285,
            ContentFormat::ApplicationPkcs10 => 286,
            ContentFormat::ApplicationPkixCert => 287,
            ContentFormat::ApplicationAifCbor => 290,
            ContentFormat::ApplicationAifJson => 291,
            ContentFormat::ApplicationSenmlXML => 310,
            ContentFormat::ApplicationSensmlXML => 311,
            ContentFormat::ApplicationSenmlEtchJson => 320,
            ContentFormat::ApplicationSenmlEtchCbor => 322,
            ContentFormat::ApplicationYangDataCbor => 340,
            ContentFormat::ApplicationYangDataCborName => 341,
            ContentFormat::ApplicationTdJson => 432,
            ContentFormat::ApplicationVoucherCoseCbor => 836,
            ContentFormat::ApplicationVndOcfCbor => 10000,
            ContentFormat::ApplicationOscore => 10001,
            ContentFormat::ApplicationJavascript => 10002,
            ContentFormat::ApplicationJsonDeflate => 11050,
            ContentFormat::ApplicationCborDeflate => 11060,
            ContentFormat::ApplicationVndOmaLwm2mTlv => 11542,
            ContentFormat::ApplicationVndOmaLwm2mJson => 11543,
            ContentFormat::ApplicationVndOmaLwm2mCbor => 11544,
            ContentFormat::TextCss => 20000,
            ContentFormat::ImageSvgXml => 30000,
        }
    }
}

/// The values of the observe option.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ObserveOption {
    Register,
    Deregister,
}

impl TryFrom<usize> for ObserveOption {
    type Error = InvalidObserve;

    fn try_from(number: usize) -> Result<ObserveOption, InvalidObserve> {
        match number {
            0 => Ok(ObserveOption::Register),
            1 => Ok(ObserveOption::Deregister),
            _ => Err(InvalidObserve),
        }
    }
}

impl From<ObserveOption> for usize {
    fn from(observe: ObserveOption) -> usize {
        match observe {
            ObserveOption::Register => 0,
            ObserveOption::Deregister => 1,
        }
    }
}

/// The CoAP packet.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Packet {
    pub header: Header,
    token: Vec<u8>,
    pub(crate) options: BTreeMap<u16, LinkedList<Vec<u8>>>,
    pub payload: Vec<u8>,
}

/// An iterator over the options of a packet.
pub type Options<'a> =
    alloc::collections::btree_map::Iter<'a, u16, LinkedList<Vec<u8>>>;

impl Packet {
    /// Maximum allowed packet size. By default limited to 1280 so that CoAP
    /// packets can be sent over TCP or UDP.
    #[cfg(not(feature = "udp"))]
    pub const MAX_SIZE: usize = 1280;

    /// Maximum allowed packet size.
    #[cfg(feature = "udp")]
    pub const MAX_SIZE: usize = 64_000;

    /// Creates a new packet.
    pub fn new() -> Packet {
        Default::default()
    }

    /// Returns an iterator over the options of the packet.
    pub fn options(&self) -> Options {
        self.options.iter()
    }

    /// Sets the token.
    pub fn set_token(&mut self, token: Vec<u8>) {
        self.header.set_token_length(token.len() as u8);
        self.token = token;
    }

    /// Returns the token.
    pub fn get_token(&self) -> &[u8] {
        &self.token
    }

    /// Sets an option's values.
    pub fn set_option(&mut self, tp: CoapOption, value: LinkedList<Vec<u8>>) {
        self.options.insert(tp.into(), value);
    }

    /// Sets an option's values using a structured option value format.
    pub fn set_options_as<T: OptionValueType>(
        &mut self,
        tp: CoapOption,
        value: LinkedList<T>,
    ) {
        let raw_value = value.into_iter().map(|x| x.into()).collect();
        self.set_option(tp, raw_value);
    }

    /// Returns an option's values.
    pub fn get_option(&self, tp: CoapOption) -> Option<&LinkedList<Vec<u8>>> {
        self.options.get(&tp.into())
    }

    /// Returns an option's values all decoded using the specified structured
    /// option value format.
    pub fn get_options_as<T: OptionValueType>(
        &self,
        tp: CoapOption,
    ) -> Option<LinkedList<Result<T, IncompatibleOptionValueFormat>>> {
        self.get_option(tp).map(|options| {
            options
                .iter()
                .map(|raw_value| T::try_from(raw_value.clone()))
                .collect()
        })
    }

    /// Returns an option's first value as a convenience when only one is
    /// expected.
    pub fn get_first_option(&self, tp: CoapOption) -> Option<&Vec<u8>> {
        self.options
            .get(&tp.into())
            .and_then(|options| options.front())
    }

    /// Returns an option's first value as a convenience when only one is
    /// expected.
    pub fn get_first_option_as<T: OptionValueType>(
        &self,
        tp: CoapOption,
    ) -> Option<Result<T, IncompatibleOptionValueFormat>> {
        self.get_first_option(tp)
            .map(|value| T::try_from(value.clone()))
    }

    /// Adds an option value.
    pub fn add_option(&mut self, tp: CoapOption, value: Vec<u8>) {
        let num = tp.into();
        if let Some(list) = self.options.get_mut(&num) {
            list.push_back(value);
            return;
        }

        let mut list = LinkedList::new();
        list.push_back(value);
        self.options.insert(num, list);
    }

    /// Adds an option value using a structured option value format.
    pub fn add_option_as<T: OptionValueType>(
        &mut self,
        tp: CoapOption,
        value: T,
    ) {
        self.add_option(tp, value.into());
    }

    /// Removes an option.
    pub fn clear_option(&mut self, tp: CoapOption) {
        if let Some(list) = self.options.get_mut(&tp.into()) {
            list.clear()
        }
    }

    /// Removes all options.
    pub fn clear_all_options(&mut self) {
        self.options.clear()
    }

    /// Sets the content-format.
    pub fn set_content_format(&mut self, cf: ContentFormat) {
        let content_format: u16 = u16::try_from(usize::from(cf)).unwrap();
        self.add_option_as(
            CoapOption::ContentFormat,
            OptionValueU16(content_format),
        );
    }

    /// Returns the content-format.
    pub fn get_content_format(&self) -> Option<ContentFormat> {
        self.get_first_option_as::<OptionValueU16>(CoapOption::ContentFormat)
            .and_then(|option| option.ok())
            .map(|value| usize::from(value.0))
            .and_then(|value| ContentFormat::try_from(value).ok())
    }

    /// Sets the value of the observe option.
    pub fn set_observe_value(&mut self, value: u32) {
        self.clear_option(CoapOption::Observe);
        self.add_option_as(CoapOption::Observe, OptionValueU32(value));
    }

    /// Returns the value of the observe option.
    pub fn get_observe_value(
        &self,
    ) -> Option<Result<u32, IncompatibleOptionValueFormat>> {
        self.get_first_option_as::<OptionValueU32>(CoapOption::Observe)
            .map(|option| option.map(|value| value.0))
    }

    /// Decodes a byte slice and constructs the equivalent packet.
    pub fn from_bytes(buf: &[u8]) -> Result<Packet, MessageError> {
        let header_result = HeaderRaw::try_from(buf);
        match header_result {
            Ok(raw_header) => {
                let header = Header::from_raw(&raw_header);
                let token_length = header.get_token_length();
                let options_start: usize = 4 + token_length as usize;

                if token_length > 8 {
                    return Err(MessageError::InvalidTokenLength);
                }

                if options_start > buf.len() {
                    return Err(MessageError::InvalidTokenLength);
                }

                let token = buf[4..options_start].to_vec();

                let mut idx = options_start;
                let mut options_number = 0u16;
                let mut options: BTreeMap<u16, LinkedList<Vec<u8>>> =
                    BTreeMap::new();
                while idx < buf.len() {
                    let byte = buf[idx];

                    if byte == 255 || idx > buf.len() {
                        break;
                    }

                    let mut delta = (byte >> 4) as u16;
                    let mut length = (byte & 0xF) as usize;

                    idx += 1;

                    // Check for special delta characters
                    match delta {
                        13 => {
                            if idx >= buf.len() {
                                return Err(MessageError::InvalidOptionLength);
                            }
                            delta = buf[idx] as u16 + 13;
                            idx += 1;
                        }
                        14 => {
                            if idx + 1 >= buf.len() {
                                return Err(MessageError::InvalidOptionLength);
                            }

                            delta = u16::from_be(u8_to_unsigned_be!(
                                buf,
                                idx,
                                idx + 1,
                                u16
                            ))
                            .checked_add(269)
                            .ok_or(MessageError::InvalidOptionDelta)?;
                            idx += 2;
                        }
                        15 => {
                            return Err(MessageError::InvalidOptionDelta);
                        }
                        _ => {}
                    };

                    // Check for special length characters
                    match length {
                        13 => {
                            if idx >= buf.len() {
                                return Err(MessageError::InvalidOptionLength);
                            }

                            length = buf[idx] as usize + 13;
                            idx += 1;
                        }
                        14 => {
                            if idx + 1 >= buf.len() {
                                return Err(MessageError::InvalidOptionLength);
                            }

                            length = (u16::from_be(u8_to_unsigned_be!(
                                buf,
                                idx,
                                idx + 1,
                                u16
                            ))
                            .checked_add(269)
                            .ok_or(MessageError::InvalidOptionLength)?)
                            .into();
                            idx += 2;
                        }
                        15 => {
                            return Err(MessageError::InvalidOptionLength);
                        }
                        _ => {}
                    };

                    options_number = options_number
                        .checked_add(delta)
                        .ok_or(MessageError::InvalidOptionDelta)?;

                    let end = idx + length;
                    if end > buf.len() {
                        return Err(MessageError::InvalidOptionLength);
                    }
                    let options_value = buf[idx..end].to_vec();

                    options
                        .entry(options_number)
                        .or_default()
                        .push_back(options_value);

                    idx += length;
                }

                let payload = if idx < buf.len() {
                    buf[(idx + 1)..buf.len()].to_vec()
                } else {
                    Vec::new()
                };

                Ok(Packet {
                    header,
                    token,
                    options,
                    payload,
                })
            }
            Err(_) => Err(MessageError::InvalidHeader),
        }
    }

    /// Returns a vector of bytes representing the Packet.
    pub fn to_bytes(&self) -> Result<Vec<u8>, MessageError> {
        self.to_bytes_internal(Some(Self::MAX_SIZE))
    }

    /// Returns a vector of bytes representing the Packet, using a custom
    /// `limit` instead of [`Packet::MAX_SIZE`] for the message size check.
    pub fn to_bytes_with_limit(
        &self,
        limit: usize,
    ) -> Result<Vec<u8>, MessageError> {
        self.to_bytes_internal(Some(limit))
    }

    /// Returns a vector of bytes representing the Packet, skipping the message
    /// size check against [`Packet::MAX_SIZE`].
    pub fn to_bytes_unlimited(&self) -> Result<Vec<u8>, MessageError> {
        self.to_bytes_internal(None)
    }

    fn to_bytes_internal(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<u8>, MessageError> {
        let mut options_delta_length = 0;
        let mut options_bytes: Vec<u8> = Vec::new();
        for (number, value_list) in self.options.iter() {
            for value in value_list.iter() {
                let mut header: Vec<u8> = Vec::with_capacity(1 + 2 + 2);
                let delta = number - options_delta_length;

                let mut byte: u8 = 0;
                if delta <= 12 {
                    byte |= (delta << 4) as u8;
                } else if delta < 269 {
                    byte |= 13 << 4;
                } else {
                    byte |= 14 << 4;
                }
                if value.len() <= 12 {
                    byte |= value.len() as u8;
                } else if value.len() < 269 {
                    byte |= 13;
                } else {
                    byte |= 14;
                }
                header.push(byte);

                if delta > 12 && delta < 269 {
                    header.push((delta - 13) as u8);
                } else if delta >= 269 {
                    let fix = delta - 269;
                    header.push((fix >> 8) as u8);
                    header.push((fix & 0xFF) as u8);
                }

                if value.len() > 12 && value.len() < 269 {
                    header.push((value.len() - 13) as u8);
                } else if value.len() >= 269 {
                    let fix = (value.len() - 269) as u16;
                    header.push((fix >> 8) as u8);
                    header.push((fix & 0xFF) as u8);
                }

                options_delta_length += delta;

                options_bytes.reserve(header.len() + value.len());
                unsafe {
                    use core::ptr;
                    let buf_len = options_bytes.len();
                    ptr::copy(
                        header.as_ptr(),
                        options_bytes.as_mut_ptr().add(buf_len),
                        header.len(),
                    );
                    ptr::copy(
                        value.as_ptr(),
                        options_bytes.as_mut_ptr().add(buf_len + header.len()),
                        value.len(),
                    );
                    options_bytes
                        .set_len(buf_len + header.len() + value.len());
                }
            }
        }

        let mut buf_length = 4 + self.payload.len() + self.token.len();
        if self.header.code != MessageClass::Empty && !self.payload.is_empty()
        {
            buf_length += 1;
        }
        buf_length += options_bytes.len();

        if limit.is_some() && buf_length > limit.unwrap() {
            return Err(MessageError::InvalidPacketLength);
        }

        let mut buf: Vec<u8> = Vec::with_capacity(buf_length);
        let header_result = self.header.to_raw().serialize_into(&mut buf);

        match header_result {
            Ok(_) => {
                buf.reserve(self.token.len() + options_bytes.len());
                unsafe {
                    use core::ptr;
                    let buf_len = buf.len();
                    ptr::copy(
                        self.token.as_ptr(),
                        buf.as_mut_ptr().add(buf_len),
                        self.token.len(),
                    );
                    ptr::copy(
                        options_bytes.as_ptr(),
                        buf.as_mut_ptr().add(buf_len + self.token.len()),
                        options_bytes.len(),
                    );
                    buf.set_len(
                        buf_len + self.token.len() + options_bytes.len(),
                    );
                }

                if self.header.code != MessageClass::Empty
                    && !self.payload.is_empty()
                {
                    buf.push(0xFF);
                    buf.reserve(self.payload.len());
                    unsafe {
                        use core::ptr;
                        let buf_len = buf.len();
                        ptr::copy(
                            self.payload.as_ptr(),
                            buf.as_mut_ptr().add(buf.len()),
                            self.payload.len(),
                        );
                        buf.set_len(buf_len + self.payload.len());
                    }
                }
                Ok(buf)
            }
            Err(_) => Err(MessageError::InvalidHeader),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{header, option_value::OptionValueString};
    use alloc::borrow::ToOwned;

    #[test]
    fn test_decode_packet_with_options() {
        let buf = [
            0x44, 0x01, 0x84, 0x9e, 0x51, 0x55, 0x77, 0xe8, 0xb2, 0x48, 0x69,
            0x04, 0x54, 0x65, 0x73, 0x74, 0x43, 0x61, 0x3d, 0x31,
        ];
        let packet = Packet::from_bytes(&buf);
        assert!(packet.is_ok());
        let packet = packet.unwrap();
        assert_eq!(packet.header.get_version(), 1);
        assert_eq!(packet.header.get_type(), header::MessageType::Confirmable);
        assert_eq!(packet.header.get_token_length(), 4);
        assert_eq!(
            packet.header.code,
            header::MessageClass::Request(header::RequestType::Get)
        );
        assert_eq!(packet.header.message_id, 33950);
        assert_eq!(*packet.get_token(), vec![0x51, 0x55, 0x77, 0xE8]);
        assert_eq!(packet.options.len(), 2);

        let uri_path = packet.get_option(CoapOption::UriPath);
        assert!(uri_path.is_some());
        let uri_path = uri_path.unwrap();
        let mut expected_uri_path = LinkedList::new();
        expected_uri_path.push_back("Hi".as_bytes().to_vec());
        expected_uri_path.push_back("Test".as_bytes().to_vec());
        assert_eq!(*uri_path, expected_uri_path);

        let uri_query = packet.get_option(CoapOption::UriQuery);
        assert!(uri_query.is_some());
        let uri_query = uri_query.unwrap();
        let mut expected_uri_query = LinkedList::new();
        expected_uri_query.push_back("a=1".as_bytes().to_vec());
        assert_eq!(*uri_query, expected_uri_query);
    }

    #[test]
    fn test_decode_packet_with_payload() {
        let buf = [
            0x64, 0x45, 0x13, 0xFD, 0xD0, 0xE2, 0x4D, 0xAC, 0xFF, 0x48, 0x65,
            0x6C, 0x6C, 0x6F,
        ];
        let packet = Packet::from_bytes(&buf);
        assert!(packet.is_ok());
        let packet = packet.unwrap();
        assert_eq!(packet.header.get_version(), 1);
        assert_eq!(
            packet.header.get_type(),
            header::MessageType::Acknowledgement
        );
        assert_eq!(packet.header.get_token_length(), 4);
        assert_eq!(
            packet.header.code,
            header::MessageClass::Response(header::ResponseType::Content)
        );
        assert_eq!(packet.header.message_id, 5117);
        assert_eq!(*packet.get_token(), vec![0xD0, 0xE2, 0x4D, 0xAC]);
        assert_eq!(packet.payload, "Hello".as_bytes().to_vec());
    }

    #[test]
    fn test_encode_packet_with_options() {
        let mut packet = Packet::new();
        packet.header.set_version(1);
        packet.header.set_type(header::MessageType::Confirmable);
        packet.header.code =
            header::MessageClass::Request(header::RequestType::Get);
        packet.header.message_id = 33950;
        packet.set_token(vec![0x51, 0x55, 0x77, 0xE8]);
        packet.add_option(CoapOption::UriPath, b"Hi".to_vec());
        packet.add_option(CoapOption::UriPath, b"Test".to_vec());
        packet.add_option(CoapOption::UriQuery, b"a=1".to_vec());
        assert_eq!(
            packet.to_bytes().unwrap(),
            vec![
                0x44, 0x01, 0x84, 0x9e, 0x51, 0x55, 0x77, 0xe8, 0xb2, 0x48,
                0x69, 0x04, 0x54, 0x65, 0x73, 0x74, 0x43, 0x61, 0x3d, 0x31
            ]
        );
    }

    #[test]
    fn test_encode_packet_with_payload() {
        let mut packet = Packet::new();
        packet.header.set_version(1);
        packet.header.set_type(header::MessageType::Acknowledgement);
        packet.header.code =
            header::MessageClass::Response(header::ResponseType::Content);
        packet.header.message_id = 5117;
        packet.set_token(vec![0xD0, 0xE2, 0x4D, 0xAC]);
        packet.payload = "Hello".as_bytes().to_vec();
        assert_eq!(
            packet.to_bytes().unwrap(),
            vec![
                0x64, 0x45, 0x13, 0xFD, 0xD0, 0xE2, 0x4D, 0xAC, 0xFF, 0x48,
                0x65, 0x6C, 0x6C, 0x6F
            ]
        );
    }

    #[test]
    fn test_encode_decode_content_format() {
        let mut packet = Packet::new();
        packet.set_content_format(ContentFormat::TextPlain);
        assert_eq!(
            ContentFormat::TextPlain,
            packet.get_content_format().unwrap()
        )
    }

    #[test]
    fn test_encode_decode_content_format_without_msb() {
        let mut packet = Packet::new();
        packet.set_content_format(ContentFormat::ApplicationJSON);
        assert_eq!(
            ContentFormat::ApplicationJSON,
            packet.get_content_format().unwrap()
        )
    }

    #[test]
    fn test_encode_decode_content_format_with_msb() {
        let mut packet = Packet::new();
        packet.set_content_format(ContentFormat::ApplicationSensmlXML);
        assert_eq!(
            ContentFormat::ApplicationSensmlXML,
            packet.get_content_format().unwrap()
        )
    }

    #[test]
    fn test_decode_empty_content_format() {
        let packet = Packet::new();
        assert!(packet.get_content_format().is_none());
    }

    #[test]
    fn option() {
        for i in 0..512 {
            assert_eq!(i, CoapOption::from(i).into());
        }
    }

    #[test]
    fn content_format() {
        for i in 0..512 {
            if let Ok(o) = ContentFormat::try_from(i) {
                assert_eq!(i, o.into());
            }
        }
    }

    #[test]
    fn observe_option() {
        for i in 0..8 {
            if let Ok(o) = ObserveOption::try_from(i) {
                assert_eq!(i, o.into());
            }
        }
    }

    #[test]
    fn options() {
        let mut p = Packet::new();
        p.add_option(CoapOption::UriHost, vec![0]);
        p.add_option(CoapOption::UriPath, vec![1]);
        p.add_option(CoapOption::ETag, vec![2]);
        p.clear_option(CoapOption::ETag);
        assert_eq!(3, p.options().len());

        let bytes = p.to_bytes().unwrap();
        let mut pp = Packet::from_bytes(&bytes).unwrap();
        assert_eq!(2, pp.options().len());

        let mut values = LinkedList::new();
        values.push_back(vec![3]);
        values.push_back(vec![4]);
        pp.set_option(CoapOption::Oscore, values);
        assert_eq!(3, pp.options().len());
    }

    #[test]
    fn test_option_u32_format() {
        let mut p = Packet::new();
        let option_key = CoapOption::Observe;
        let values = vec![0, 100, 1000, 10000, u32::MAX];
        for &value in &values {
            p.add_option_as(option_key, OptionValueU32(value));
        }
        let expected = values.iter().map(|&x| Ok(OptionValueU32(x))).collect();
        let actual = p.get_options_as::<OptionValueU32>(option_key);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn test_option_utf8_format() {
        let mut p = Packet::new();
        let option_key = CoapOption::UriPath;
        let values = vec!["", "simple", "unicode 😁 stuff"];
        for &value in &values {
            p.add_option_as(option_key, OptionValueString(value.to_owned()));
        }
        let expected = values
            .iter()
            .map(|&x| Ok(OptionValueString(x.to_owned())))
            .collect();
        let actual = p.get_options_as::<OptionValueString>(option_key);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn observe() {
        let mut p = Packet::new();
        assert_eq!(None, p.get_observe_value());
        p.set_observe_value(0);
        assert_eq!(Some(Ok(0)), p.get_observe_value());
    }

    #[test]
    fn to_bytes_limits_work() {
        let mut packet = Packet::new();

        packet.payload = vec![0u8; 1200];
        assert!(packet.to_bytes().is_ok());

        packet.payload = vec![0u8; 1300];
        assert_eq!(packet.to_bytes(), Err(MessageError::InvalidPacketLength));
        assert!(packet.to_bytes_with_limit(1380).is_ok());
        assert!(packet.to_bytes_unlimited().is_ok());
    }

    #[test]
    fn option_delta_u8_overflow() {
        // Build a packet with options 1 and 258, which have a delta of 257.
        // Although 257 does not fit into a u8, it does fit into the
        // 1-byte extended option delta, because that is biased by 13.
        //
        // coap_lite 0.13.1 and earlier decoded this delta incorrectly.
        let mut input = Packet::new();
        let option_1 = CoapOption::IfMatch;
        let option_258 = CoapOption::NoResponse;

        input.add_option(option_1, vec![0]);
        input.add_option(option_258, vec![1]);
        let bytes = input.to_bytes().unwrap();

        // Verify everything round-trips
        let output = Packet::from_bytes(&bytes).unwrap();
        assert_eq!(output.options().len(), 2);
        assert_eq!(output.get_first_option(option_1), Some(vec![0]).as_ref());
        assert_eq!(
            output.get_first_option(option_258),
            Some(vec![1]).as_ref()
        );
    }

    #[test]
    fn reject_excessive_option_delta() {
        // The 2-byte extended option delta field is biased by 269, and
        // therefore can represent values as high as 0x1_010C. Deltas
        // larger than 0x0_FFFF are illegal and should be rejected.
        //
        // coap_lite 0.13.1 and earlier did not reject this input and
        // instead presented an incorrect set of options.
        let bytes = [
            // header
            0x40, 0x01, 0x00, 0x00,
            // option delta = 0x1_0000, option length = 0
            0xe0, 0xfe, 0xf3,
        ];

        let result = Packet::from_bytes(&bytes);
        assert_eq!(result, Err(MessageError::InvalidOptionDelta));
    }

    #[test]
    fn reject_excessive_option_number() {
        // It's possible to arrive at an option number > 0xFFFF by
        // adding deltas. Option numbers > 0xFFFF are illegal and
        // should be rejected.
        //
        // coap_lite 0.13.1 and earlier did not reject this input and
        // instead presented an incorrect set of options.
        let bytes = [
            // header
            0x40, 0x01, 0x00, 0x00,
            // option delta = 0xFFFF, option length = 0
            0xe0, 0xfe, 0xf2,
            // option delta = 0x01, option length = 0
            0x10,
        ];

        let result = Packet::from_bytes(&bytes);
        assert_eq!(result, Err(MessageError::InvalidOptionDelta));
    }
}
