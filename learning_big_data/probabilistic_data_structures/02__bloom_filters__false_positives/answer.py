
def calculate_error_rate(bf, true_negatives):
    false_positives_count = 0
    for word in true_negatives:
        if word in bf:
            false_positives_count += 1

    return false_positives_count / len(true_negatives)    