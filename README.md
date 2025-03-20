# QLoRA Deep Gaussian Processes 
This repository contains code for training deep gaussian processes for VaR prediction conditioned on LLM embeddings, i.e. LLaMa-2 finetuned using QLoRA. Finetuning data is retrieved using the newsdata API.  

Value at Risk (VaR) is optimized based on a portfolio with weights <img src="https://latex.codecogs.com/svg.image?\large&space;&space;w_i" /> and assets <img src="https://latex.codecogs.com/svg.image?\large&space;&space;f_i" /> represented as deep gaussian processes:

<img src="https://latex.codecogs.com/svg.image?\LARGE&space;\begin{matrix}f=\sum_iw_if_i\\f_i\sim\textrm{GP}(\mu_i,K)\end{matrix}" />

where the kernel <img src="https://latex.codecogs.com/svg.image?\large&space;&space;K" /> has a deep kernel representation

<img src="https://latex.codecogs.com/svg.image?\large&space;&space;\begin{matrix}k(x_i,x_j)=\textrm{exp}(-\frac{1}{2}||x_i-x_j||/l^2)\\k_{ij}=k(\hat{x}_i,\hat{x}_j)\end{matrix}" />

a user-specified risk level ($\alpha$ = 0 is risk-free, $\alpha$ = 1 is maximal risk) [[1]](https://arxiv.org/pdf/2105.06126) 


<img src="https://latex.codecogs.com/svg.latex?\Large&space;V_{\alpha}(\mathbf{x},\mathbf{z})=\textrm{inf}\{\omega:P(f(\mathbf{x},\mathbf{z})\leq\omega)\geq\alpha\}" title="\Large V_{\alpha}(\mathbf{x},\mathbf{z})=\textrm{inf}\{\omega:P(f((\mathbf{x},\mathbf{z}))\leq\omega)\geq\alpha\}" />

where $f(\mathbf{x},\mathbf{z})$ is a deep GP conditioned on a context embedding $\mathbf{z}$ generated by our finetuned LLaMa model. The VaR is defined as the minimum upper bound $\omega$ required such that $f(\mathbf{x},\mathbf{z})$ is less than the specified risk level.

## How to Use This Repository

1. First install the requirements:
   ```
   pip install -r requirements.txt
   ```
   
3. Collect text from the newsdata API by running
   ```
   python grab_news_text.py
   ```
   The text retrieved can be filtered by country, category, or language with the params argument.
   
5. Preprocess and filter raw text data using
   ```
   python fineweb-2-pipeline.py --dataset="path/to/your/dataset"
   ```
   
7. Finetune with
   ```
   python qlora.py --dataset="path/to/your/dataset"
   ```
   
9. Train deep GP with
    ```
   python dgp.py
    ```
