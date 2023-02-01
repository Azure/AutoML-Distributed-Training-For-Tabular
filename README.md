## ❗Important

**Features contained in this repository are in private preview. Preview versions are provided without a service level agreement, and they are not recommended for production workloads. Certain features might not be supported or might have constrained capabilities. For more information, see [Supplemental Terms of Use for Microsoft Azure Previews](https://azure.microsoft.com/en-us/support/legal/preview-supplemental-terms/).**

## Welcome
Welcome to the Private Preview release of AutoML distributed training for Tabular workloads.

## What is 'AutoML distributed training for Tabular workloads'?
By tabular workloads, we refer to workloads that are nlp or image workloads. These workloads typically consists of mixture of datatypes such as number, categorical, datetime and string datatypes. AutoML currently support 3 types of tasks for this workload, namely regression, classification and forecasting.
When tabular data is large, a single node training often causes out of memory failures or is too slow to be useful. In such cases, distributed training is likely to build a successful model relatively quickly as AutoML will distribute data among multiple nodes and perform distributed featurization and distributed training.  

## Characteristics of distributed training in AutoML
<ul>
	<li><b>Data size support:</b> For classification, regression - upto 1TB. For forecasting - upto 200GB.</li>
	<li><b>Models supported:</b> For classification, regression - Distributed LightGBM. For forecasting - Distributed TCN.</li>
	<li><b>How to enable this feature?:</b> This feature can be enabled by simply adding few configurations to AutoMLConfig. Please follow the samples provided. Note that this feature is currently not supported through UI or CLI or V2 SDK. It is only supported through V1 SDK. However general availability of this feature will be only be made available through V2 SDK. </li>	
	<li><b>Incompatible features:</b> 'Cross validation', 'ONNX', "Code-gen", "Ensembling" features are not compatible with distributed training. Also AutoML may make  choices such as restricting available featurizers, subsampling data used for validation, explainability and model evaluation.  Other experiences such as model registration, model deployment should work as expected. </li>
</ul>

## Contact
For any question regarding AzureML Foundation models, please contact azmlfoundationmodels@microsoft.com

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
