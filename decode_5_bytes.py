message = b'\x00\x00\x00\x00j'


def printRegSchema(data):
    result = bytearray()
    if len(data) >= 5:
        for i in range(1, 5):
            result.append(data[i])

    id = int.from_bytes(result, byteorder='big', signed=False);
    print("bingo: ", result,' = ', id)

printRegSchema(message)