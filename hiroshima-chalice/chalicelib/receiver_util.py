import boto3, io, struct
from ebmlite import loadSchema
from enum import Enum
from botocore.config import Config
from decimal import Decimal
import time
from datetime import datetime
from .helper.log_elapsed_time import log_elapsed_time

def decimal_to_int(obj):
    if isinstance(obj, Decimal):
        return int(obj)

class Mkv(Enum):
    SEGMENT = 0x18538067
    CLUSTER = 0x1F43B675
    SIMPLEBLOCK = 0xA3

class Ebml(Enum):
    EBML = 0x1A45DFA3

class KVSParser:
    def __init__(self, media_content):
        self.__stream = media_content["Payload"]
        self.__schema = loadSchema("matroska.xml")
        self.__buffer = bytearray()

    @property
    def fragments(self):
        return [fragment for chunk in self.__stream if (fragment := self.__parse(chunk))]

    def __parse(self, chunk):
        self.__buffer.extend(chunk)
        header_elements = [e for e in self.__schema.loads(self.__buffer) if e.id == Ebml.EBML.value]
        if header_elements:
            fragment_dom = self.__schema.loads(self.__buffer[:header_elements[0].offset])
            self.__buffer = self.__buffer[header_elements[0].offset:]
            return fragment_dom

def get_simple_blocks(media_content):
    parser = KVSParser(media_content)
    return [b.value for document in parser.fragments for b in 
            next(filter(lambda c: c.id == Mkv.CLUSTER.value, 
                        next(filter(lambda s: s.id == Mkv.SEGMENT.value, document)))) 
            if b.id == Mkv.SIMPLEBLOCK.value]

def create_audio_sample(simple_blocks, margin=4):
    position = 0
    total_length = sum(len(block) - margin for block in simple_blocks)
    combined_samples = bytearray(total_length)
    for block in simple_blocks:
        temp = block[margin:]
        combined_samples[position:position+len(temp)] = temp
        position += len(temp)
    return combined_samples

def pcm_s16le_to_pcm_mulaw(pcm_data):
    """16-bit PCM を Mu-Law に変換"""
    import audioop
    mulaw_data = audioop.lin2ulaw(pcm_data, 2)  # 2はサンプルのバイト数（16-bit PCM）
    return mulaw_data

def convert_bytearray_to_wav(samples):
    mulaw_samples = pcm_s16le_to_pcm_mulaw(samples)
    length = len(mulaw_samples)
    channel = 1
    bit_par_sample = 8
    format_code = 7
    sample_rate = 8000
    header_size = 44
    wav = bytearray(header_size + length)
    
    wav[0:4] = b"RIFF"
    wav[4:8] = struct.pack("<I", 36 + length)
    wav[8:12] = b"WAVE"
    wav[12:16] = b"fmt "
    wav[16:20] = struct.pack("<I", 16)
    wav[20:22] = struct.pack("<H", format_code)
    wav[22:24] = struct.pack("<H", channel)
    wav[24:28] = struct.pack("<I", sample_rate)
    wav[28:32] = struct.pack("<I", sample_rate * channel * bit_par_sample // 8)
    wav[32:34] = struct.pack("<H", channel * bit_par_sample // 8)
    wav[34:36] = struct.pack("<H", bit_par_sample)
    wav[36:40] = b"data"
    wav[40:44] = struct.pack("<I", length)
    
    wav[44:] = mulaw_samples
    return wav

def create_kvs_client():
    region_name = "ap-northeast-1"
    return boto3.client("kinesisvideo", region_name=region_name)

def create_archive_media_client(ep):
    region_name = "ap-northeast-1"
    return boto3.client("kinesis-video-archived-media", endpoint_url=ep, config=Config(region_name=region_name))

def upload_audio_to_s3(bucket_name, audio_data, filename):
    region_name = "ap-northeast-1"
    s3_client = boto3.client("s3", region_name=region_name)
    try :
        s3_client.upload_fileobj(
            io.BytesIO(audio_data),
            bucket_name,
            filename,
            ExtraArgs={"ContentType": "audio/wav"}
        )
        return True
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False

def get_media_data(arn, start_timestamp, end_timestamp):
    start_time = time.time()
    log_elapsed_time("receiver: Starting get_media_data", start_time)

    kvs_client = create_kvs_client()

    list_frags_ep = kvs_client.get_data_endpoint(StreamARN=arn, APIName="LIST_FRAGMENTS")["DataEndpoint"]
    list_frags_client = create_archive_media_client(list_frags_ep)
    log_elapsed_time("receiver: Created LIST_FRAGMENTS client", start_time)

    fragment_list = list_frags_client.list_fragments(
        StreamARN = arn, 
        FragmentSelector = {
            "FragmentSelectorType": "PRODUCER_TIMESTAMP",
            "TimestampRange": {"StartTimestamp": datetime.fromtimestamp(start_timestamp), "EndTimestamp": datetime.fromtimestamp(end_timestamp)}
        }
    )
    log_elapsed_time("receiver: Listed fragments", start_time)

    sorted_fragments = sorted(fragment_list["Fragments"], key = lambda fragment: fragment["ProducerTimestamp"])
    fragment_number_array = [fragment["FragmentNumber"] for fragment in sorted_fragments]

    get_media_ep = kvs_client.get_data_endpoint(StreamARN=arn, APIName="GET_MEDIA_FOR_FRAGMENT_LIST")["DataEndpoint"]
    get_media_client = create_archive_media_client(get_media_ep)
    log_elapsed_time("receiver: Created GET_MEDIA client", start_time)

    media = get_media_client.get_media_for_fragment_list(StreamARN = arn, Fragments = fragment_number_array)
    log_elapsed_time("receiver: Retrieved media data", start_time)
    
    return media

def save_audio_to_tmp(wav_audio, filename="output.wav"):
    with open(f"/tmp/{filename}", "wb") as out_file:
        out_file.write(wav_audio)
