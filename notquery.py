import json
import csv

results = []

# This is the AQL Query that I run for NOT queries (it is an OR query)
# let startNodes = (FOR n in [nodes][**] FILTER UPPER(n.Label) =~ UPPER('cbb') RETURN n)
# let left_paths = (FOR n IN startNodes FOR v, e, p IN 1..1 ANY n GRAPH 'RC1' FILTER UPPER(p.vertices[0].Label) =~ UPPER('cbb')
#     AND p.edges[0].Type == 'BC Conventional Synapse' RETURN {paths: p})
#
# let right_paths = (FOR n IN startNodes FOR v, e, p IN 1..1 ANY n GRAPH 'RC1' FILTER UPPER(p.vertices[0].Label) =~ UPPER('cbb')
#     AND p.edges[0].Type == 'Ribbon Synapse' RETURN {paths: p})
#
#     RETURN {left: left_paths[**].paths, right: right_paths[**].paths}

# The intersection-results-20210916.json was my local copy of the query results
with open('intersection-results-20210916.json') as f:
    results = json.load(f)

left = []
left_second_node = []
dict = {}
left_count = 0
for row in results[0]["left"]:
    left_count += 1
    comparator = str(row['vertices'][0]['_key']) + str(row['vertices'][1]['_key'])
    dict[comparator] = dict.get(comparator, [])
    dict[comparator].append({'Node1': row['vertices'][0]['_key'], 'Node1 Label': row['vertices'][0]['Label'], 'Edge':  row['edges'][0]['_key'], 'Edge Type': row['edges'][0]['Type'], 'Node2': row['vertices'][1]['_key'], 'Node2 Label': row['vertices'][1]['Label']})
    left.append(comparator)
    left_second_node.append(str(row['vertices'][1]['_key']))

right = []
right_second_node = []
right_count = 0
for row in results[0]["right"]:
    right_count +=1
    comparator = str(row['vertices'][0]['_key']) + str(row['vertices'][1]['_key'])
    dict[comparator] = dict.get(comparator, [])
    dict[comparator].append({'Node1': row['vertices'][0]['_key'], 'Node1 Label': row['vertices'][0]['Label'], 'Edge':  row['edges'][0]['_key'], 'Edge Type': row['edges'][0]['Type'], 'Node2': row['vertices'][1]['_key'], 'Node2 Label': row['vertices'][1]['Label']})
    right.append(comparator)
    right_second_node.append(str(row['vertices'][1]['_key']))

# Sanity check that all nodes accounted for
# print(left_count + right_count)


# Find the NOT difference between first query and second query
diff = set(left) - set(right)

# Find the NOT difference between 2nd node keys
second_diff = set(left_second_node) - set(right_second_node)

csv_output1 = []
csv_output2 = []
# Store the differences
for d in diff:
    csv_output1.append(dict[d][0])

# Filter the difference based on 2nd difference
for d in diff:
    if dict[d][0]['Node2'] in second_diff:
        csv_output2.append(dict[d][0])

# Create the CSV outputs
# This is the output that Crystal wants to see
keys = csv_output1[0].keys()
with open('difference-20210916.csv', 'w', newline='') as f:
    dict_writer = csv.DictWriter(f, keys)
    dict_writer.writeheader()
    dict_writer.writerows(csv_output1)

# This was another question she asked but can be ignored at the moment
with open('difference2-20210916.csv', 'w', newline='') as f:
    dict_writer = csv.DictWriter(f, keys)
    dict_writer.writeheader()
    dict_writer.writerows(csv_output2)

# Export all the results sans filters
# Also not a necessary output, I used this for validation
all = []
for d in dict.values():
    for item in d:
        all.append(item)

with open('all-queries-20210916.csv', 'w', newline='') as f:
    dict_writer = csv.DictWriter(f, keys)
    dict_writer.writeheader()
    dict_writer.writerows(all)
