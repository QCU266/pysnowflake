import generator

with open('id.txt', 'r') as fp_read, open('decode_base62id.txt', 'w') as fp_write:
    for line in fp_read:
        line = generator.SnowFlakeId.decode_base62id(line.strip())
        fp_write.write("%s:%d:%d:%d:%d\n" % (line.base62id(), line.timestamp, line.mac_addr, line.pid, line.sequence))
        
