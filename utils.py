def get_text(block, block_map):
    text = ""
    if 'Relationships' in block:
        for rel in block['Relationships']:
            if rel['Type'] == 'CHILD':
                for cid in rel['Ids']:
                    word = block_map[cid]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + " "
    return text.strip()


def safe_int(value):
    try:
        return int(value.strip())
    except:
        return None