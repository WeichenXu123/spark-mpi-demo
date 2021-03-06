from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import horovod.tensorflow as hvd

import random
import pyarrow as pa

learn = tf.contrib.learn

tf.logging.set_verbosity(tf.logging.INFO)

def cnn_model_fn(features, labels, mode):
    """Model function for CNN."""
    # Input Layer
    # Reshape X to 4-D tensor: [batch_size, width, height, channels]
    # MNIST images are 28x28 pixels, and have one color channel
    input_layer = tf.reshape(features["x"], [-1, 28, 28, 1])

    # Convolutional Layer #1
    # Computes 32 features using a 5x5 filter with ReLU activation.
    # Padding is added to preserve width and height.
    # Input Tensor Shape: [batch_size, 28, 28, 1]
    # Output Tensor Shape: [batch_size, 28, 28, 32]
    conv1 = tf.layers.conv2d(
        inputs=input_layer,
        filters=32,
        kernel_size=[5, 5],
        padding="same",
        activation=tf.nn.relu)

    # Pooling Layer #1
    # First max pooling layer with a 2x2 filter and stride of 2
    # Input Tensor Shape: [batch_size, 28, 28, 32]
    # Output Tensor Shape: [batch_size, 14, 14, 32]
    pool1 = tf.layers.max_pooling2d(inputs=conv1, pool_size=[2, 2], strides=2)

    # Convolutional Layer #2
    # Computes 64 features using a 5x5 filter.
    # Padding is added to preserve width and height.
    # Input Tensor Shape: [batch_size, 14, 14, 32]
    # Output Tensor Shape: [batch_size, 14, 14, 64]
    conv2 = tf.layers.conv2d(
        inputs=pool1,
        filters=64,
        kernel_size=[5, 5],
        padding="same",
        activation=tf.nn.relu)

    # Pooling Layer #2
    # Second max pooling layer with a 2x2 filter and stride of 2
    # Input Tensor Shape: [batch_size, 14, 14, 64]
    # Output Tensor Shape: [batch_size, 7, 7, 64]
    pool2 = tf.layers.max_pooling2d(inputs=conv2, pool_size=[2, 2], strides=2)

    # Flatten tensor into a batch of vectors
    # Input Tensor Shape: [batch_size, 7, 7, 64]
    # Output Tensor Shape: [batch_size, 7 * 7 * 64]
    pool2_flat = tf.reshape(pool2, [-1, 7 * 7 * 64])

    # Dense Layer
    # Densely connected layer with 1024 neurons
    # Input Tensor Shape: [batch_size, 7 * 7 * 64]
    # Output Tensor Shape: [batch_size, 1024]
    dense = tf.layers.dense(inputs=pool2_flat, units=1024, activation=tf.nn.relu)

    # Add dropout operation; 0.6 probability that element will be kept
    dropout = tf.layers.dropout(
        inputs=dense, rate=0.4, training=mode == tf.estimator.ModeKeys.TRAIN)

    # Logits layer
    # Input Tensor Shape: [batch_size, 1024]
    # Output Tensor Shape: [batch_size, 10]
    logits = tf.layers.dense(inputs=dropout, units=10)

    predictions = {
        # Generate predictions (for PREDICT and EVAL mode)
        "classes": tf.argmax(input=logits, axis=1),
        # Add `softmax_tensor` to the graph. It is used for PREDICT and by the
        # `logging_hook`.
        "probabilities": tf.nn.softmax(logits, name="softmax_tensor")
    }
    if mode == tf.estimator.ModeKeys.PREDICT:
        export_outputs = {'predict_output': tf.estimator.export.PredictOutput(predictions)}
        return tf.estimator.EstimatorSpec(mode=mode, predictions=predictions,export_outputs=export_outputs)

    # Calculate Loss (for both TRAIN and EVAL modes)
    onehot_labels = tf.one_hot(indices=tf.cast(labels, tf.int32), depth=10)
    loss = tf.losses.softmax_cross_entropy(
        onehot_labels=onehot_labels, logits=logits)

    # Configure the Training Op (for TRAIN mode)
    if mode == tf.estimator.ModeKeys.TRAIN:
        # Horovod: scale learning rate by the number of workers.
        optimizer = tf.train.MomentumOptimizer(
            learning_rate=0.001 * hvd.size(), momentum=0.9)

        # Horovod: add Horovod Distributed Optimizer.
        optimizer = hvd.DistributedOptimizer(optimizer)

        train_op = optimizer.minimize(
            loss=loss,
            global_step=tf.train.get_global_step())
        return tf.estimator.EstimatorSpec(mode=mode, loss=loss, train_op=train_op)

    # Add evaluation metrics (for EVAL mode)
    eval_metric_ops = {
        "accuracy": tf.metrics.accuracy(
            labels=labels, predictions=predictions["classes"])}
    return tf.estimator.EstimatorSpec(
        mode=mode, loss=loss, eval_metric_ops=eval_metric_ops)


def load_pandas_df(filePath):
    with pa.OSFile(filePath, 'rb') as f:
        buff = f.read_buffer()
    context = pa.default_serialization_context()
    return context.deserialize(buff)


def load_pyarrow_table(filePath):
    with pa.OSFile(filePath, 'rb') as f:
        reader = pa.RecordBatchFileReader(f)
        return reader.read_all()


def main(argv):

    # read args from command `mpirun ... python hvd_run_mnist_training.py inputFilePath outputFilePath`
    inputFilePath = argv[1]
    #outputFilePath = argv[2]
    exportModelDir = argv[2]

    # Horovod: initialize Horovod.
    hvd.init()

    # Load training and eval data
    table = load_pyarrow_table(inputFilePath)

    # later I will change code to avoid using `to_pandas`
    pdf = table.to_pandas()
    train_data = np.reshape(np.array(np.concatenate(pdf['features']), dtype=np.float32), (-1, 784))
    train_labels = np.array(pdf['label'], dtype=np.float32)

    # Horovod: pin GPU to be used to process local rank (one GPU per process)
    config = tf.ConfigProto()
    config.gpu_options.allow_growth = True
    config.gpu_options.visible_device_list = str(hvd.local_rank())

    # Horovod: save checkpoints only on worker 0 to prevent other workers from
    # corrupting them.
    model_dir = './mnist_convnet_model_' + str(random.randint(0, 2<<30))\
        if hvd.rank() == 0 else None

    # Create the Estimator
    mnist_classifier = tf.estimator.Estimator(
        model_fn=cnn_model_fn, model_dir=model_dir,
        config=tf.estimator.RunConfig(session_config=config))

    # Set up logging for predictions
    # Log the values in the "Softmax" tensor with label "probabilities"
    tensors_to_log = {"probabilities": "softmax_tensor"}
    logging_hook = tf.train.LoggingTensorHook(
        tensors=tensors_to_log, every_n_iter=500)

    # Horovod: BroadcastGlobalVariablesHook broadcasts initial variable states from
    # rank 0 to all other processes. This is necessary to ensure consistent
    # initialization of all workers when training is started with random weights or
    # restored from a checkpoint.
    bcast_hook = hvd.BroadcastGlobalVariablesHook(0)

    # Train the model
    train_input_fn = tf.estimator.inputs.numpy_input_fn(
        x={"x": train_data},
        y=train_labels,
        batch_size=100,
        num_epochs=None,
        shuffle=True)

    # Horovod: adjust number of steps based on number of GPUs.
    mnist_classifier.train(
        input_fn=train_input_fn,
        steps=5 // hvd.size(),
        hooks=[logging_hook, bcast_hook])

    """
    feature_x = tf.feature_column.numeric_column("x", [784])
    feature_columns = [feature_x]
    feature_spec = tf.feature_column.make_parse_example_spec(feature_columns)
    serving_input_receiver_fn = tf.estimator.export.build_parsing_serving_input_receiver_fn(feature_spec)
    """

    """
    def serving_input_receiver_fn():
        serialized_tf_example = tf.placeholder(dtype=tf.string,
                                               shape=[None],
                                               name='input_tensors')
        receiver_tensors = {'inputs': serialized_tf_example}
        feature_spec = {'x': tf.FixedLenFeature([784],tf.float32)}
        features = tf.parse_example(serialized_tf_example, feature_spec)
        return tf.estimator.export.ServingInputReceiver(features, receiver_tensors)
    """

    """
    with open(outputFilePath, "w") as f:
        varlist = mnist_classifier.get_variable_names()
        vars = {}
        for var in varlist:
            vars[var] = mnist_classifier.get_variable_value(var).tolist()
        # the result is large (135MB). only store keys to output file for now.
        f.write(str(varlist))
    """

    def serving_input_receiver_fn():
        # The outer dimension (None) allows us to batch up inputs for
        # efficiency. However, it also means that if we want a prediction
        # for a single instance, we'll need to wrap it in an outer list.
        inputs = {"x": tf.placeholder(shape=[None, 784], dtype=tf.float32)}
        return tf.estimator.export.ServingInputReceiver(inputs, inputs)
    mnist_classifier.export_savedmodel(exportModelDir, serving_input_receiver_fn)


if __name__ == "__main__":
    tf.app.run()
