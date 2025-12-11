import random
import math
import numpy as np

class DataConfig:
    name: str
    num_ops: int
    insert_ratio: float
    add_ratio: float
    transition_to_updates_ratio: float
    correct_lookup_ratio: float
    
    def __init__(
        self, 
        name: str, 
        num_ops: int, 
        insert_ratio: float, 
        add_ratio: float, 
        transition_to_updates_ratio: float, 
        correct_lookup_ratio: float):
        
        self.name = name
        self.num_ops = num_ops
        self.insert_ratio = insert_ratio
        self.add_ratio = add_ratio
        self.transition_to_updates_ratio = transition_to_updates_ratio
        self.correct_lookup_ratio = correct_lookup_ratio

class Item:
    key: np.uint64
    value: np.uint64
    
def generate_data(config: DataConfig, output_file: str):
    
    item_history: list[Item] = []
    
    keys_history: set[np.uint64] = set()
    
    rng = np.random.default_rng()
    
    u64_max = 2**64 - 2
    
    with open(output_file, 'w') as f:
        for i in range(config.num_ops):
            if random.random() < config.insert_ratio:
                # insert
                if len(item_history) > 0 and random.random() < config.add_ratio - (math.pow((i / config.num_ops), 2) * config.transition_to_updates_ratio):
                    update_idx = random.randint(0, len(item_history) - 1)
                    
                    new_val: np.uint64 = rng.integers(0, 2**64 - 2, dtype=np.uint64) # type: ignore
                    item_history[update_idx].value = new_val
                    
                    f.write(f"I {item_history[update_idx].key} {new_val}\n")
                else:
                    item = Item()
                    
                    item.key = rng.integers(0, u64_max, dtype=np.uint64)
                    item.value = rng.integers(0, u64_max, dtype=np.uint64)
                    
                    keys_history.add(item.key) # type: ignore
                    item_history.append(item)
                    
                    f.write(f"I {item.key} {item.value}\n")
            else:
                if len(item_history) > 0 and random.random() < config.correct_lookup_ratio:
                    lookup_idx = random.randint(0, len(item_history) - 1)
                    
                    lookup_key = item_history[lookup_idx].key
                    lookup_value = item_history[lookup_idx].value
                    
                    f.write(f"L {lookup_key} {lookup_value}\n")
                else:
                    potential_key: np.uint64
                    
                    while True:
                        potential_key = rng.integers(0, u64_max, dtype=np.uint64)
                        
                        if not keys_history.intersection([potential_key]):
                            break
                    
                    value = rng.integers(0, u64_max, dtype=np.uint64)
                    f.write(f"L {potential_key} {value}\n")

if __name__ == "__main__":
    
    config_list: list[DataConfig] = [
        DataConfig(
            name="balanced.txt",
            num_ops=100000,
            insert_ratio=0.5,
            add_ratio=0.5,
            transition_to_updates_ratio=0,
            correct_lookup_ratio=1
        ),
        DataConfig(
            name="write_heavy.txt",
            num_ops=100000,
            insert_ratio=0.9,
            add_ratio=0.5,
            transition_to_updates_ratio=0,
            correct_lookup_ratio=1
        ),
        DataConfig(
            name="read_heavy.txt",
            num_ops=100000,
            insert_ratio=0.1,
            add_ratio=0.5,
            transition_to_updates_ratio=0,
            correct_lookup_ratio=1
        ),
        DataConfig(
            name="typical.txt",
            num_ops=100000,
            insert_ratio=0.5,
            add_ratio=0.8,
            transition_to_updates_ratio=0.8,
            correct_lookup_ratio=1
        ),
        DataConfig(
            name="typical_with_misses.txt",
            num_ops=100000,
            insert_ratio=0.5,
            add_ratio=0.8,
            transition_to_updates_ratio=0.8,
            correct_lookup_ratio=0.9
        ),
        DataConfig(
            name="large.txt",
            num_ops=1000000,
            insert_ratio=0.5,
            add_ratio=0.8,
            transition_to_updates_ratio=0.8,
            correct_lookup_ratio=0.9
        ),
    ]
    
    for config in config_list:
        generate_data(config, "./datasets/"+config.name)