import sys

ids = set()
line_num = 0
with open(sys.argv[1], 'r') as fp:
	for line in fp.xreadlines():
		line_num += 1
		line = line.strip()
		if line in ids:
			print "%d: %s" % (line_num, line)
		else:
			ids.add(line)
