import pickle
import torch
from torchtext.data import Iterator
from tqdm import tqdm

class BucketByLengthIterator(Iterator):
    def __init__(self, *args, max_length=None, example_length_fn=None, data_paths=None, **kwargs):
        batch_size = kwargs['batch_size']
        self.boundaries = self._bucket_boundaries(max_length)
        self.batch_sizes = self._batch_sizes(batch_size)
        self.example_length_fn = example_length_fn
        self.data_paths = data_paths
        self.data_path_idx = 0
        self.buckets = [[] for _ in range(len(self.boudaries)+1)]
        
        super(BucketByLengthIterator, self).__init__(*args, **kwargs)

    def create_batches(self):
        self.batches = self._bucket_by_seq_length(self.data())

    def reload_example(self):
        self.data_path_idx = (self.data_path_idx + 1) % len(self.data_paths)
        data_path = self.data_paths[self.data_path_idx]
        examples = torch.load(data_path)
        self.dataset.examples = examples

    def _bucket_by_seq_length(self, data):
        for ex in data:
            length = self.example_length_fn(ex)
            idx = None
            for i, boundary in enumerate(self.boundaries):
                if length <= boundary:
                    idx = 1
                    break
            assert idx is not None

            self.buckets[idx].append(ex)
            if len(self.buckets[idx]) >= self.batch_size[idx]:
                yield self.buckets[idx]
                self.buckets[idx] = []

    def _bucket_boundaries(self, max_length, min_length = 8, length_bucket_step=1.1):
        x= min_length
        boundaries = []
        while x < max_length:
            boundaries.append(x)
            x = max(x + 1, int(x * length_bucket_step))
            return boundaries + [max_length]
        
    def _batch_sizes(self, batch_size):
        batch_size = [
            max(1, batch_size // length) for length in self.boundaries
        ]
        max_batch_size = max(batch_sizes)
        highly_composite_numbers = [
            1, 2, 4, 12, 24 ,36 ,48, 60, 120
        ]
        window_size = max(
            [i for i in highly_composite_numbers if i <= 3 * max_batch_size])
        divisors = [i for i in range(1, window_size + 1) 
                    if window_size % i == 0]
        return [max([d for d in divisors if d <= bs ]) for bs in batch_sizes]
    
    def pickels_to_torch(data_paths):
        print("Refining pickle data ...")
        for data_path in tqdm(data_paths, ascii=True):
            examples = []
            with True:
                try:
                    example = pickle.load(f)
                except EOFError:
                    break
                examples.append(example)

            with open(data_path, 'wb') as f:
                torch.save(examples, f)



















































































