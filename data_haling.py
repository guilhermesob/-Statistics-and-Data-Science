import pandas as pd
import numpy as np
import random
from typing import List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def generate_large_dataset(size: int) -> pd.DataFrame:
    """
    Generates a large dataset with random values.

    Args:
        size (int): Number of rows in the dataset.

    Returns:
        pd.DataFrame: Generated dataset.

    Raises:
        ValueError: If dataset size is not positive.
    """
    if size <= 0:
        raise ValueError("Dataset size must be positive.")
    
    data = {
        "ID": np.arange(1, size + 1),
        "Value": [random.uniform(-1000, 1000) if random.random() > 0.1 else None for _ in range(size)],
        "Category": [random.choice(["A", "B", "C", None]) for _ in range(size)],
    }
    logging.info(f"Generated dataset with {size} entries.")
    return pd.DataFrame(data)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and validates data.

    Args:
        df (pd.DataFrame): Dataset to be cleaned.

    Returns:
        pd.DataFrame: Cleaned dataset.

    Raises:
        ValueError: If dataset is None.
    """
    if df is None:
        raise ValueError("Dataset cannot be None.")
    
    original_size = len(df)
    df = df.drop_duplicates()
    df["Value"] = df["Value"].fillna(df["Value"].mean())
    df = df[df["Value"] >= 0]
    df["Category"] = df["Category"].fillna("Unknown")
    cleaned_size = len(df)
    
    logging.info(f"Cleaned data: {original_size} -> {cleaned_size} entries.")
    return df


def merge_sort(arr: List[float]) -> List[float]:
    """
    Sorts a list using Merge Sort.

    Args:
        arr (List[float]): List to be sorted.

    Returns:
        List[float]: Sorted list.

    Raises:
        ValueError: If list is None.
    """
    if arr is None:
        raise ValueError("List cannot be None.")
    
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left, right)


def merge(left: List[float], right: List[float]) -> List[float]:
    """
    Merges two sorted lists into a single sorted list.

    Args:
        left (List[float]): First sorted list.
        right (List[float]): Second sorted list.

    Returns:
        List[float]: Merged sorted list.
    """
    sorted_array = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            sorted_array.append(left[i])
            i += 1
        else:
            sorted_array.append(right[j])
            j += 1
    sorted_array.extend(left[i:])
    sorted_array.extend(right[j:])
    return sorted_array


def data_pipeline(size: int):
    """
    Executes the data pipeline.

    Args:
        size (int): Number of rows in the dataset.
    """
    try:
        # Step 1: Generate data
        df = generate_large_dataset(size)
        logging.info(f"Original dataset preview:\n{df.head()}")

        # Step 2: Clean data
        clean_df = clean_data(df)
        logging.info(f"Cleaned dataset preview:\n{clean_df.head()}")

        # Step 3: Sort values
        sorted_values = merge_sort(clean_df["Value"].tolist())
        logging.info(f"First sorted values: {sorted_values[:10]}")

        # Final Report
        logging.info(f"Pipeline completed: {len(df)} original entries -> {len(clean_df)} cleaned entries.")
    except Exception as e:
        logging.error(f"Pipeline error: {e}")


if __name__ == "__main__":
    data_pipeline(size=100000)
