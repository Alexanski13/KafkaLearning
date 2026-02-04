package com.alexanski.estore.transfers.service;

import com.alexanski.estore.transfers.model.TransferRestModel;

public interface TransferService {
    public boolean transfer(TransferRestModel productPaymentRestModel);
}
