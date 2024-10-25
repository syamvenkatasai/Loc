from torch.nn import CrossEntropyLoss

from transformers import LayoutLMTokenizer
from app.unilm.layoutlm.layoutlm.data.funsd import FunsdDataset, InputFeatures
from torch.utils.data import DataLoader, RandomSampler, SequentialSampler
from transformers import LayoutLMForTokenClassification
import torch
from transformers import AdamW
from tqdm import tqdm
from transformers import BertForSequenceClassification
from ace_logger import Logging
logging = Logging(name="prediction_api")


def train_and_evaluate():
    logging.info(f'In train and evaluate')

    def get_labels(path):
        with open(path, "r") as f:
            labels = f.read().splitlines()
        if "O" not in labels:
            labels = ["O"] + labels
        return labels

    labels = get_labels("/var/www/prediction_api/app/data/invoice_data/labels.txt")
    num_labels = len(labels)
    logging.info(f'Labels obtained from data/invoice_data/labels.txt are {labels} num_labels {num_labels}')
    label_map = {i: label for i, label in enumerate(labels)}
    # Use cross entropy ignore index as padding label id so that only real label ids contribute to the loss later
    pad_token_label_id = CrossEntropyLoss().ignore_index


    args = {'local_rank': -1,
            'overwrite_cache': True,
            'data_dir': r"/var/www/prediction_api/app/data/invoice_data",
            'model_name_or_path':'microsoft/layoutlm-base-uncased',
            'max_seq_length': 512,
            'model_type': 'layoutlm',}


    # class to turn the keys of a dict into attributes (thanks Stackoverflow)
    class AttrDict(dict):
        def __init__(self, *args, **kwargs):
            super(AttrDict, self).__init__(*args, **kwargs)
            self.__dict__ = self

    args = AttrDict(args)

    tokenizer = LayoutLMTokenizer.from_pretrained("microsoft/layoutlm-base-uncased")

    # the LayoutLM authors already defined a specific FunsdDataset, so we are going to use this here
    train_dataset = FunsdDataset(args, tokenizer, labels, pad_token_label_id, mode="train")
    train_sampler = RandomSampler(train_dataset)
    train_dataloader = DataLoader(train_dataset,
                                sampler=train_sampler,
                                batch_size=2)

    eval_dataset = FunsdDataset(args, tokenizer, labels, pad_token_label_id, mode="test")
    eval_sampler = SequentialSampler(eval_dataset)
    eval_dataloader = DataLoader(eval_dataset,
                                sampler=eval_sampler,
                                batch_size=2)


    logging.info(f'preprocessing done: length of training set{len(train_dataloader)}')
    logging.info(f'preprocessing done: length of testing set{len(eval_dataloader)}')


    batch = next(iter(train_dataloader))
    input_ids = batch[0][0]
    tokenizer.decode(input_ids)


    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = LayoutLMForTokenClassification.from_pretrained("microsoft/layoutlm-base-uncased", num_labels=num_labels)
    model.to(device)

    logging.info(f" Preprocessing completed..proceeding with training")


    def training(model_path):
        from transformers import AdamW
        from tqdm import tqdm
        
        optimizer = AdamW(model.parameters(), lr=5e-5)

        global_step = 0
        num_train_epochs = 20
        t_total = len(train_dataloader) * num_train_epochs # total number of training steps 
        logging.info(f'In training... epoch are {num_train_epochs} total steps are {t_total}')

        #put the model in training mode
        model.train()
        for epoch in range(num_train_epochs):
            for batch in tqdm(train_dataloader, desc="Training"):
                input_ids = batch[0].to(device)
                bbox = batch[4].to(device)
                attention_mask = batch[1].to(device)
                token_type_ids = batch[2].to(device)
                labels = batch[3].to(device)

                # forward pass
                outputs = model(input_ids=input_ids, bbox=bbox, attention_mask=attention_mask, token_type_ids=token_type_ids,
                                labels=labels)
                loss = outputs.loss

                # print loss every 100 steps
                if global_step % 100 == 0:
                    print(f"Loss after {global_step} steps: {loss.item()}")

                # backward pass to get the gradients 
                loss.backward()

                #print("Gradients on classification head:")
                #print(model.classifier.weight.grad[6,:].sum())

                # update
                optimizer.step()
                optimizer.zero_grad()
                global_step += 1
        
        PATH=model_path
        torch.save(model.state_dict(), PATH)
        logging.info(f"Model file saved to  {PATH}.")

    def evaluation():
        import numpy as np
        from seqeval.metrics import (
            classification_report,
            f1_score,
            precision_score,
            recall_score,
        )

        eval_loss = 0.0
        nb_eval_steps = 0
        preds = None
        out_label_ids = None

        # put model in evaluation mode
        model.eval()
        for batch in tqdm(eval_dataloader, desc="Evaluating"):
            with torch.no_grad():
                input_ids = batch[0].to(device)
                bbox = batch[4].to(device)
                attention_mask = batch[1].to(device)
                token_type_ids = batch[2].to(device)
                labels = batch[3].to(device)

                # forward pass
                outputs = model(input_ids=input_ids, bbox=bbox, attention_mask=attention_mask, token_type_ids=token_type_ids,
                                labels=labels)
                # get the loss and logits
                tmp_eval_loss = outputs.loss
                logits = outputs.logits

                eval_loss += tmp_eval_loss.item()
                nb_eval_steps += 1

                # compute the predictions
                if preds is None:
                    preds = logits.detach().cpu().numpy()
                    out_label_ids = labels.detach().cpu().numpy()
                else:
                    preds = np.append(preds, logits.detach().cpu().numpy(), axis=0)
                    out_label_ids = np.append(
                        out_label_ids, labels.detach().cpu().numpy(), axis=0
                    )

        # compute average evaluation loss
        eval_loss = eval_loss / nb_eval_steps
        preds = np.argmax(preds, axis=2)

        out_label_list = [[] for _ in range(out_label_ids.shape[0])]
        preds_list = [[] for _ in range(out_label_ids.shape[0])]

        for i in range(out_label_ids.shape[0]):
            for j in range(out_label_ids.shape[1]):
                if out_label_ids[i, j] != pad_token_label_id:
                    out_label_list[i].append(label_map[out_label_ids[i][j]])
                    preds_list[i].append(label_map[preds[i][j]])

        results = {
            "loss": eval_loss,
            "precision": precision_score(out_label_list, preds_list),
            "recall": recall_score(out_label_list, preds_list),
            "f1": f1_score(out_label_list, preds_list),
        }
        logging.info(f'Evaluation:{results}')
        return results



    output_path='/var/www/prediction_api/data/model_files/Invoice_model.pt'
    train=training(output_path)
    logging.info(f"Training Done.... Proceeding to Evaluation")
    evalute=evaluation()
    logging.info("Evaluation Done")




