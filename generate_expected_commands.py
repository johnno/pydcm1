# Generate the full list of expected protocol queries for 8 zones, 4 groups, 8 sources, and 8 line inputs each, with \r
expected = []
for z in range(1, 9):
    expected.append(f"b'<Z{z},LQ/>\r'")  # zone label
    expected.append(f"b'<Z{z}.MU,LQ/>\r'")  # zone volume
    expected.append(f"b'<Z{z}.MU,SQ/>\r'")  # zone source
    for l in range(1, 9):
        expected.append(f"b'<Z{z}.L{l},Q/>\r'")  # zone line input enable
for g in range(1, 5):
    expected.append(f"b'<G{g},LQ/>\r'")  # group label
    expected.append(f"b'<G{g},Q/>\r'")  # group status
    expected.append(f"b'<G{g}.MU,LQ/>\r'")  # group volume
    expected.append(f"b'<G{g}.MU,SQ/>\r'")  # group source
    for l in range(1, 9):
        expected.append(f"b'<G{g}.L{l},Q/>\r'")  # group line input enable
for s in range(1, 9):
    expected.append(f"b'<L{s},LQ/>\r'")  # source label
with open('expected_commands.txt', 'w') as f:
    for cmd in sorted(expected):
        f.write(cmd + '\n')
