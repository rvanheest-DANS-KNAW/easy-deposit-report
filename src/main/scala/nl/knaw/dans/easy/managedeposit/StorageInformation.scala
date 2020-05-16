package nl.knaw.dans.easy.managedeposit

case class StorageInformation(depositId: DepositId,
                              numberOfContinuedDeposits: Int,
                              storageSpace: Long,
                             )
