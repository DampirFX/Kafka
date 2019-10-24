import ctypes

class JsonSerializer:
    def __init__(self, path):
        self.dll = ctypes.WinDLL(path)

    def version(self):
        funcVersion = self.dll.Version
        funcVersion.restype = ctypes.c_char_p

        return funcVersion().decode('utf-8')

    def toAvroJson(self, schema, input):
        funcMemoryFree = self.dll.MemoryFree
        funcSerialize = self.dll.SerializeJsonToAvroJson
        funcSerialize.restype = ctypes.c_bool

        c_schema = ctypes.c_char_p(schema.encode('ascii'));
        c_input = ctypes.c_char_p(input);
        c_inputLen = ctypes.c_uint(len(c_input.value))
        c_output = ctypes.c_char_p(None)
        c_outputLen = ctypes.c_uint(0)
        c_error = ctypes.c_char_p(None)

        if not funcSerialize(c_schema, c_input, c_inputLen, ctypes.byref(c_output), ctypes.byref(c_outputLen), ctypes.byref(c_error)):
            error = c_error.value
            funcMemoryFree(c_error);
            raise Exception(error)

        result = c_output.value
        funcMemoryFree(c_output);
        return result;

    def toAvroBinaryBase64(self, schema, input):
        funcMemoryFree = self.dll.MemoryFree
        funcSerialize = self.dll.SerializeJsonToAvroBinaryBase64
        funcSerialize.restype = ctypes.c_bool

        c_schema = ctypes.c_char_p(schema.encode('ascii'));
        c_input = ctypes.c_char_p(input);
        c_inputLen = ctypes.c_uint(len(c_input.value))
        c_output = ctypes.c_char_p(None)
        c_outputLen = ctypes.c_uint(0)
        c_error = ctypes.c_char_p(None)

        if not funcSerialize(c_schema, c_input, c_inputLen, ctypes.byref(c_output), ctypes.byref(c_outputLen), ctypes.byref(c_error)):
            error = c_error.value
            funcMemoryFree(c_error);
            raise Exception(error)

        result = c_output.value
        funcMemoryFree(c_output);
        return result;


class JsonDeserializer:
    def __init__(self, path):
        self.dll = ctypes.WinDLL(path)

    def version(self):
        funcVersion = self.dll.Version
        funcVersion.restype = ctypes.c_char_p

        return funcVersion().decode('utf-8')

    def fromAvroJson(self, schema, input):
        funcMemoryFree = self.dll.MemoryFree
        funcDeserialize = self.dll.DeserializeJsonFromAvroJson
        funcDeserialize.restype = ctypes.c_bool

        c_schema = ctypes.c_char_p(schema.encode('ascii'));
        c_input = ctypes.c_char_p(input);
        c_inputLen = ctypes.c_uint(len(c_input.value))
        c_output = ctypes.c_char_p(None)
        c_outputLen = ctypes.c_uint(0)
        c_error = ctypes.c_char_p(None)

        if not funcDeserialize(c_schema, c_input, c_inputLen, ctypes.byref(c_output), ctypes.byref(c_outputLen), ctypes.byref(c_error)):
            error = c_error.value
            funcMemoryFree(c_error);
            raise Exception(error)

        result = c_output.value
        funcMemoryFree(c_output);
        return result;

    def fromAvroBinaryBase64(self, schema, input):
        funcMemoryFree = self.dll.MemoryFree
        funcDeserialize = self.dll.DeserializeJsonFromAvroBinaryBase64
        funcDeserialize.restype = ctypes.c_bool

        c_schema = ctypes.c_char_p(schema.encode('ascii'));
        c_input = ctypes.c_char_p(input);
        c_inputLen = ctypes.c_uint(len(c_input.value))
        c_output = ctypes.c_char_p(None)
        c_outputLen = ctypes.c_uint(0)
        c_error = ctypes.c_char_p(None)

        if not funcDeserialize(c_schema, c_input, c_inputLen, ctypes.byref(c_output), ctypes.byref(c_outputLen), ctypes.byref(c_error)):
            error = c_error.value
            funcMemoryFree(c_error);
            raise Exception(error)

        result = c_output.value
        funcMemoryFree(c_output);
        return result;