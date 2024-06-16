# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
from datetime import datetime
from os import environ

from airflow import DAG
from airflow.decorators import task_group, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.operators.comprehend import ComprehendStartPiiEntitiesDetectionJobOperator, \
    ComprehendCreateDocumentClassifierOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.comprehend import (
    ComprehendStartPiiEntitiesDetectionJobCompletedSensor, ComprehendCreateDocumentClassifierCompletedSensor,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

# Creating a creating custom document classifier takes 40min to 1hr. If SKIP_DOCUMENT_CLASSIFIER
# is True then document classifier will be skipped. This way we can still have
# the code snippets for docs, and we can manually run the full tests.
SKIP_DOCUMENT_CLASSIFIER = environ.get("SKIP_DOCUMENT_CLASSIFIER", default=True)

ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_comprehend_document_classifier"
INPUT_S3_KEY = "input-data/training-doc.csv"
#https://amazon-textract-public-content.s3.amazonaws.com/analyzeid/driverlicense.png
SAMPLE_DATA = """
INSURANCE_ID,IN NETWORK COVERAGE ONLY ambetter superior FROM healthplan TDI TM Subscriber  Member  ID  UXXXXXXXXX Effective Date of Plan Ambetter Balanced Care 1  Vision Coverage   Adult Dental Rx BIN  008019 Copays PCP Coinsurance MedRX Specialist Deductible MedRx ER Rx GenericBrand
INSURANCE_ID,Anthem Anthem Core DirectAccoss eacg EPO BlueCross Sample Identification Number J000 Effective Date  Deductible 5000510000 Contract Code 00000 OOP 6350512700 Rx Bin 000000 CoInsurance Tier 130 Tier 250 PCN A4 Office Visit 3 OV 60 then Rx Group WLHA Ded and 30 Coins Plan 040 Rx Tier1Rx Tier2 Ded then 19550 Rx Tier3Rx Tier4 Ded then 7530 Select Rx List Pediatric Dental Prime Pathway Tiered EPO
INSURANCE_ID,UnitedHealthcare R Health Plan Member ID Group Number Member PCP Payer ID PCP Phone OPTUMRx Rx Bin Copays Rx PCN Office ER Tier 1 OV RX Grp UrgCare Spec Tier 1 SpecOV Referrals Required UnitedHealthcare NexusACO R DOI0508 Underwritten by Appropriate Legal Entity
INSURANCE_ID,1 INSURANCE COMPANY NAME COVERAGE TYPE 2 MEMBER NAME  4 EFFECTIVE DATE XXXXXXXX MEMBER NUMBER XXXXXXXXX 3 GROUP  XXXXXXXXXXXX PRESCRIPTION GROUP  xxxxx PCP COPAY 1500 PRESCRIPTION COPAY SPECIALIST COPAY 2500 1500 GENERIC EMERGENCY ROOM COPAY 7500 2000 NAME BRAND 5 MEMBER SERVICES 1800XXXXXXX CLAIMSINQUIRIES 1800XXXXXXX
INSURANCE_ID,Blue Cross Blue Shield R R Enrollee Name FIRST M LASTNAME  Enrollee ID RxBIN 004336 DZW920000000 RxGrp RX4655 Issuer  Blue Dental Blue Vision R R
INSURANCE_ID,Molina Marketplace TDI MOLINA ID  HEALTHCARE Member DOB Plan Subscriber Name Subscriber ID Provider Provider Phone Provider Group Medical Cost Share Prescription Drugs Primary Care Generic Drugs Specialist Visits Preferred Brand Drugs Urgent Care NonPreferred Brand Drugs ER Visit Specialty Drugs Molina Healthcare Rx Bin Rx PCN Rx Group
INSURANCE_ID,North Garolina Subscriber BERNICE D SAMPLE 01 State Health Plan FOR TEACHERS AND STATE EMPLOYEES A Division of the Department of State Treasurer Subscriber ID Treasurer  CPA XXXXXXXXXXX InNetwork Member Copay Northampton County Sch Date Issued Group No Selected PCP 10  S27130 PCPMental HlthSubst Abuse 25 RXBIN RXPCN RXGRP Specialist 80 004336 ADV RX0274 PhyOccuSpch TherapyChiro 52 Primary Care Provider PCP Urgent Care 70 None Selected ER 300  Ded  20 same for outofnetwork BlueOptions 8020 Plan  Deductible BlueE PPO Paid for by YOU and other  Taxpayers
INSURANCE_ID,Statewide Network Hometown The preferred provider network for members residing in  Renown Health medical facilities for emergency Member Number Healths and hospital care Open Access HMO Visit  Member Name Plan Nurse Advice Line  or  Subscriber Name Submit claims to  ID 88023 Mail claims to Hometown Health Member services BenefitsID card   Member Login at  Contract ID XXX  799981703 OR download mobile app  ERUrgent care facilities only outside NV Multiplan  Eligibility benefit or precertification information Visit  HometownR RxBIN 019059  or  or  RxPCN 07570000 RxGrp HTH Pharmacy services  Possession of this card does not guarantee eligibility
INSURANCE_ID,Health Insurance Generic Inc Health Plan 123456 123456789 Member ID 123456789 Group number 123456 Member individual de Generic Inc Employee Plan Employee  56A8 Anytown  Payer ID 123456AB Dependents Employee spouse 56CD Rx BIN 123456 Employee child A 56EF RX GRP GENERIC Employee child B Copays Office visit 20 ER50 Specialist 560 Health Insurance Co PPO Underwritten by Health Insurance Co
INSURANCE_ID,TEXAS LIABILITY INSURANCE CARD COMPANY PHONE NUMBER COMPANY  OLD REPUBLIC INSURANCE CO COMPANY NUMBER 24147 POLICY NUMBER EFFECTIVE DATE EXPIRATION DATE MWTB 313468   YEAR MAKEMODEL VEHICLE IDENTIFICATION NUMBER ALL OWNED AND  OR LEASED VEHICLES AGENCYCOMPANY ISSUING CARD ARI FLEET LT  INSURED BNSF RAILWAY COMPANY   L This policy provides at least the minirnurn amounts of liability insurance required by the Texas Motor Vehicle Safety Responsibility Act for the Specified vehicle and named insureds and may provide coverage for other persons and other vehicles as provided by the insurance policy SEE IMPORTANT NOTICE ON REVERSE SIDE
CMS1500,1500 HEALTH INSURANCE CLAIM FORM APPROVED BYNATIONAL UNIFORM CLAIM COMMITTEE  PICA PICA 1 MEDICARE MEDICAID TRICARE CHAMPVA GROUP FECA OTHER 10 INSUREDS ID NUMBER For Program n 1 CHAMPUS HEALTH PLAN BUKLLING Medeare  Medead  Sponsors SSN SSN or ID SSN ID 2 PATIENTS NAME Last Namo Frit Name Middlo Inittal 3 PATIENTS BIRTH DATE SEX 4 INSUREDS NAME First Name Middle Initial MM DD YY M F 5 PATIENTS ADDRESS No Street 6 PATIENT RELATIONSHIP TO INSURED 7 INSUREDS ADDRESS No Street Self Spouse Child Other CITY STATE 9 PATIENT STATUS CITY STATE Single Married Other ZIP CODE TELEPHONE include Aree Code ZIP CODE TELEPHONE include Area Code   FullTime PartTime Employed Student Student   9 OTHER INSUREDSNAME Lest Name Est Name Middle 10 IS PATIENTS CONDITION RELATED TO 11 INSUREDS POLICY GROUP OR FECA NUMBER a OTHER INSUREDS POLICY OR GROUPNUMBER a EMPLOYMENT Current or Previous a INSUREDS DATE OF BIRTH SEX MM DD YY YES NO M F b OTHER INSUREDS DATE OF BIRTH MM YY SEX b AUTO ACCIDENT DE PLACE State b EMPLOYERS NAME OR SCHOOL NAME M F YES NO c EMPLOYERS NAME OR  NAME c OTHER ACCIDENT c INSURANCE PLANNAME OR PROGRAM NAME YES NO d INSURANCE PLANNAME OR PROGRAM NAME 10d RESERVED FORLOCAL USE d is THERE ANOTHER HEALTH BENEFIT PLAN YES NO  yee return to and complete itern 9 sd READ BACK OF FORM BEFORE COMPLETING  SIGNING THIS FORM 13 INSUREDS OR AUTHORIZED PERSONS SIGNATURE  authorize 12 PATIENTS OR AUTHORIZED PEASONS SIGNATURE authortze the release of any medicator other Information necessary paymentot medical benefits to theundersigned physidian or supplier for oprocess this  also request payment of government benefis other to mysell or tothe party who accepts assignment doscribed below below SIGNED DATE SIGNED 14 DATE OF CURRENT ILLNE ss First symptom OR 15 IF PATIENT HAS HAD SAME OR SIMILAR ILINESS 16 DATES PATIENT UNABLE TO WORK IN CURRENT OCCUPATION MM DD YY INJUAY Accident OR GIVE RAST DATE MM DD YY MM DD YY MM 00 YY 4 PREGNANCYLMP FROM TO 17 NAME OF REFERRING PROVIDER OR OTHER SOURCE 17a 10 HOSPITALIZATION DATES RELATED TO CURRENT SERVICES MM DD YY MM DD YY 17b NPI FROM TO 19 RESERVED FCR LOCAL USE 20 OUTSIDE LAB CHARGES YES NO 21 DIAGNOSIS OR NATURE OF ILLNESS OR INJURY Relate Items 1 2 or to Item24E by Line 22 MEDICAID RESUEMISSION CODE ORIGINAL REF NO 1 3 L 23 PRIOR AUTHORIZATION NUMBER 2 4 24 A DATES OF SERVICE B c D PROCEDURES SERVICES OR SUPPLIES E F G H I J From To PLACEOR Explain Unusual Circurnstances DIAGNOSIS DAYS Fority ID RENDERING MM DD YY MM DD YY SERVICE EMG CPTHCPCS MODIFIER POINTER  CHARGES UNITS Ron QUAL PROVIDER ID  1 NPI 2 NPI 3 NPI 4 NPI OR 5 NPI 6 NPI 25 FEDERAL TAXID NUMBER SSN EIN 26 PATIENTS ACCOUNT NO 27 ACCEPT ASSIGNMENT 28 TOTAL CHARGE 29 AMOUNT PAID 30 BALANCE DUE gwl 000 badki YES NO    31 SIGNATURE OF HYSICIAN OR SUPPLIER 32 SERVICE FACILITY LOCATION INFORMATION 33 BILUNG PROVIDER INFO   INCLUDING DEGREESOR CREDENTIALS  I certity that the statemente on the reverse apply  part thereot SIGNED DATE a a NUCC Instruction Manual available at  OMB APPROVAL PENDING
CMS1500,HEALTH INSURANCE CLAIM FORM APPROVED BY NATIONAL UNIFORM CLAIM COMMITTEE NUCC  PICA PICA 1 MEDICARE MEDICAID TRICARE CHAMPVA GROUP FECA OTHER 1n INSUREDS ID NUMBER For Program In item 1 HEALTH PLAN BLKLUNG Medicare MedBald IDADODA Member IDI ID ID ID 2 PATIENTS NAME Laxt Name Firat Name Middle Initial 3 PATIENTS BIRTH DATE SEX 4 INSUREDS NAME Last Name First Name Middle Initial MM DO YY M F 5 PATIENTS ADDRESS No Streat 6 PATIENT RELATIONSHIP TO INSURED 7 INSUREDS ADDRESS No Street Self Spouse Child Other CITY STATE 8 RESERVED FOR NUCC USE CITY STATE ZIP CODE TELEPHONE Include Area Gode ZIP CODE TELEPHONE Include Area Code     9 OTHER INSUREDS NAME Last Namo First Name Middle Initial 10 IS PATIENTS CONDITION RELATED TO 11 INSUREDS POLICY GROUP OR FECA NUMBER a OTHER INSUREDS POLICY OR GROUP NUMBER IL EMPLOYMENT Current or Previoua a INSUREDS DATE OF BIRTH SEX MM DD YY YES NO M F b RESERVED FOR NUCC USE b AUTO ACCIDENT PLACE State b OTHER CLAIM ID Designated by NUCC YES NO AND c RESERVED FOR NUCC USE c OTHER ACCIDENT c INSURANCE PLAN NAME OR PROGRAM NAME YES NO d INSURANCE PLAN NAME OR PROGRAM NAME 10d CLAIM CODES Designated by NUCC d IS THERE ANOTHER HEALTH BENEFIT PLAN YES NO II yes complete Items 9 9a and 9d READ BACK OF FORM BEFORE COMPLETING  SIGNING THIS FORM 13 INSUREDS OR AUTHORIZED PERSONS SIGNATURE I authorize 12 PATIENTS OR AUTHORIZED PERSONS SIGNATURE I authorize the release of any modical or other Information necessary payment of medical benefits to the underalgned phyalcian or supplior for to process this claim I also request payment of government benefits either to myself or to the party who accepts assignment services described below below SIGNED DATE SIGNED 14 DATE OF CURRENT ILLNESS INJURY or PREGNANCY LMP 16 OTHER DATE 16 DATES PATIENT UNABLE TO WORK IN CURRENT OCCUPATION MM DD YY MM DD YY MM DD YY MM DD YY QUAL QUAL FROM TO 17 NAME OF REFERRING PROVIDER OR OTHER SOURCE 17a 18 HOSPITALIZATION DATES RELATED TO CURRENT SERVICES MM DD YY MM DD YY 17b NPI FROM TO 19 ADDITIONAL CLAIM INFORMATION Designated by NUCC 20 OUTSIDE LAB  CHARGES YES NO 21 DIAGNOSIS OR NATURE OF ILLNESS OR INJURY Relate AL to service line below 24E ICD Ind 22 RESUBMISSION CODE ORIGINAL REF NO A B c D 23 PRIOR AUTHORIZATION NUMBER E F G H I J K L 24 A DATES OF SERVICE B c D PROCEDURES SERVICES OR SUPPLIES E F G H I J From To PLACEOF Explain Unusual Circumstances DIAGNOSIS DAYS EPEDT OR Femily ID RENDERING MM DD YY MM DD YY SERVICE EMG CPTMCPCS MODIFIER POINTER  CHARGES UNITS Plan QUAL PROVIDER ID  1 NPI 2 NPI 3 NPI 4 NPI 5 NPI 6 NPI 25 FEDERAL TAX ID NUMBER SSN EIN 26 PATIENTS ACCOUNT NO 27 ACCEPT ASSIGNMENT 28 TOTAL CHARGE 29 AMOUNT PAID 30 Rsvd for NUCC Use For govt olaims see  YES NO   31 SIGNATURE OF PHYSICIAN OR SUPPLIER 32 SERVICE FACILITY LOCATION INFORMATION 33 BILLING PROVIDER INFO  PH  INCLUDING DEGREES OR CREDENTIALS   I certify that the statements on the reverse apply to this bIll and are made a part thereof n b a b SIGNED DATE NUCC Instruction Manual available at  PLEASE PRINT OR TYPE OMB APPROVAL PENDING
CMS1500,AETNA HEALTH INC  HEALTH INSURANCE CLAI FOR BLUE  APPROVED BY NATIONAL UNIFOR CLAN COTTISE caos nca PICA Y L EDICARE EDICAID TRICARE CHAPHIA GROUP FECA OTHER sa INBUREDS LD NUBR LAN KUNG For Program    as disonsors Altenter DE a a AET45454 2 PATIENTS NAE Last Name  some intus 3 PATIENTS BRITH DATE SEX 4 INSUREDS NAE Last Name Post Name  PORTER ONICA  F PORTER  S PATIONTS ACDRESS  street 6 PATIENT RELATIONSHEP TO INSURED 7 INSUREDS ACDRESS N  Sull Spouse Chide Other  CITY STATE  PATIENT STATUS CITY STATE  HI high arried Other  HI ZIP CODE TELEPHONE Include Area Code ZP CODE TELEPHONE Include Aven Code    Tamo Past Time Emproyed Student Student    a OTHER INSUREDS NAE Last Name Name ode to  PATIENTS CONDITION RELATED TO 11 INSUREDS POLICY GROUP OR FICA NUBRA PORTER NATALIE 142  OTHE POLICY OR GROUP  EPLOENT Currert   INSUREDS DATI OF BATH BEX AF1244543  DO Y YES NO   b OTHER INBUREOS DATE OF BRTH  SEX b AUTO DO VY PLACE State  EPLOYERS NAE OR SCHOOL NAE    VES NO  EPLOYERS NAE OR SCHOOL NAE  OTHER ACCIDENTI  INBUPANCE PLAN NAE OR PROGPA NAE ves NO FAILY PLAN  INSURANCE PLAN NAE OR AA RESERVED FOR LOCAL USE 6  THEPE ANDTHER E PLANT FAILY PLAN YES NO y return 10 and complete  9  READ BACK OF FOR SEFORE COPLITING  SICAING THIS FOR 13 INSUREDS OR UTHORIZED PERBONS SIGNATURE 12 PATIENTS OR AUTHORIZED PERSONS SIGNATURE the  al any medion or other information become of medical   the undersigned or super for lo process    request payment  government benefits    to be poft we accept assignvent services described below below SIGNED SIGNATURE ON FILE  DATE SIGNATURE ON FILE SIGNED Y 14 DATE OF CUPPENT LLINESS Pes OR is  PATIENT HAS HAD SAE OR SL AR LINESS 15 DATES PATIENT LIVABLE TO WORK N CUREENT QCOUPATION DO Y NAURY ave FRBT DATE und do W DO A FRO 17 NAE OF REPERPING PROVIDER OR OTHER SOURCE 12a 18 HOSPT SEATICE DATES PELATED TO CUBRENT sgrvices  DO Y usa DO  15th NOT PRO 10 RESERVED FORLOCAL USE 30 OUTSICE LABT  CHWRSS VES NO 21 OR NATURE OF LLINESS OR PLURY 12 300   24E by yLine 22 EDICAID PE SUBISSION ORIGINAL REF NO 002 9   23 PRICH AUTHORIZATION NUBER 2  2D46782344 24 A DATECE OF RERVICE  c D PROCEDURES services OR e à  1 a  To United DIAGADS o RENDERNO DO YV  DE TY EO PONTER  CHARGES 1867  OUN  1 12 15 11 12 15 11 11 0010 1 124 00 1 NPI 1111111111 2 NA 3 1991 4 N 5 NPI 6 NOT 25 PEDERAL TAXLD NUBR EN 20 PATIENTS ACCOUNT NO 27 accept 28 TOTAL 29 AOUNT PAID 30 BALANCE DUE back  216 YES NO  124 00  0 00  00 31 SIGNATURE OF OR SUPPLER 32 SERVICE FACUTY LOCATION TON  BLLING PROVIDER DEORBES OR CREDENTIALE ARK DAVID  DEO LOCATION    Be on the  apple bell and  spite  part ADDRESS2 ADDRESSID ADDRESS2 SIGNATURE ON FILE   ARK DAVID SIGNED  DATE  FG2FC763T6767 11111111111 Y NUCC instruction anual available at  APPROVED OB09380999 FOR CS1500 0805
CMS1500,1500 HEALTH INSURANCE CLAIM FORM of MATIOPAL 0098 III CED  DE PLAM INA then   o     ne   pey a   18 We DEX 4 HIME  C Patient Name     PATENT   APATENT ary  o ze 0006 200000   E        FO NC  ones on  A  to W   M NO e  X   AITO PLACE      NO  6 6 VES  a    a them PAAM P VES    A arrow TOPAL    PORSONG       pory  undenter       ryad re  paty   the  oare  L 14 40  the 10  MATC  16     M mow 10    en on osen   as   Y  35 n  mow to  SORLOCAL 90 E  P    W AID    786 53 250 61  414 01 465 3   A of  6  c A T Yo      YY MI 00 YY  EMA  L  1  99213 21 1 93 po 1   2   99213 21 1 33 po 1 MPL  E 3 E 12 05 oal sal 06 los  33213 21 4 18400 2  4 5  D 6   PAR               X123400  4  372 op  372 30 on   98 Bring Providar    to     ve    PO  Anytown OR DATE     NUCC instruction at  APPROVED FORM 1500
CMS1500,Access Medicare Cuatro LLC HEALTH INSURANCE CLAIM FORM  pin     orn mem               ID112345    that    My  is  M    the   fresh thread states       x         NATIONAL    Datas       and    thes 75202   the  wan       15  river can on mini  CareCore National LLC Aetna Radiology GP167890    POLIONT  NAMER  concept   or w W  0212345          are   01 INTEREST NCD         times   Numb one          on      the   novel numb GP247890      and       to can    no  on   on                                        instruct whom  MAM SIGNATURE ON FILE    SIGNATURE ON FILE 14 10        a      and mean       am     am 10   ar  meal the  with 1 g a  a  mow to 14     can   2900 th 77000   m      mm  and      ED  9  am prospect     80291XA  U 6         L       or  e  PROCEDUPEL 1 K        a    n     on none  mm  1   18   et   1500   2          17840   3   16      220   4   s   750       x AD3F2    2430 60  500  os sumper  on             Our on           on Rest    V     x are a  1234567893      mean PLEASE
CMS1500,  1500 HEALTH INSURANCE CLAIM FORM               s  0            a             2000     Norm        an       e         1 2  3 4 6  d                 2 N     cow  SAMPLE
CMS1500,HEALTH INSURANCE CLAIM FORM    mas are  na IT    are The over  name   nan as the        a  pos         a an   MOR where          more  to are      and one SAME on    MOCK of or 0000   Cater at CODK         the      NAME  marks   NAME two your    NONE  the more     am and    on       NO    SCHOOL   NO   ones  of company    e we e press  HOUPWICE us MORE all ME    new NONE    cones Companies we                the and DE        PERSONS something    de resion                                    come   work SONATURE came BOND SONATURE ON FILE   uses men  som 3 one    am      MAI 29 NONE no   am now   MARK or  movees  a  and 100   n 5   ADOTIONS NS on convention  m  scrames  to  on was  MA        cowe  6008    L   L       w    n       name  e a activities SA unda E       to   e  20  me  1  m  more  10  A  1234567890 10 11 A  1234867890 B 10 11 A  E 10 11 A w 1234967890 10 11 A  222912345 11 A   not   KNOW   a 2     more Online  me  hurt acc      DO 00  on   works  and    WORTH  5551212  10mg       ROHARO KIDARE wo              10 succ instruction    nee  TO BORCER LL can TBK AMOND FORM 1900 a 12
CMS1500,HEALTH INSURANCE CLAIM FORM APPROVED BY NATIONAL UNFORM CLAIM COMMITTEE NUCC PICA PICA 1 MEDICARE MEDICAID TRICAPE CHAMPVA OTHER ta INSUREDSID NUMBER For Progr am in item 1 PLAN Medicare Medicaida DI IDW ID 2 PATIENTS NAME Last Name First Name Mode Inital 3 PATIENTS BRTH DATE SEK 4 NSUREDS NAME Last Name First Name  MM YY   16 66 M F x  5 PATIENTS ADDRESS No Street 6 PATIENT RELATIONSHP TOINSURED 7 INSUREDS ADDRESS No Street  Self Spouse Child Other  CITY STATE 8 RESERVED FOR NUCC USE CITY STATE   OPCODE TELEPHONE Indude Area Code field 8 reserved for NUCC use ZIP CODE TELEPHONE Include Area Code     9 OTHERINBUREDS NAME Last Name First Name   IS PATIENTS CONDITION RELATED TO  INSUREDS POLICY GROUP ORFECA NUMBER  Policy123456678 a OTHER INSUREDS POLICY OR GROUP NUMBER a EMPLOYMENT Current or Prewious a INSUREDSDATE OF BIRTH SEX MM DD YY OtherPolicy12345678 YES NO  16 66 M F b RESERVED FOR NUCC USE b AUTO DACCIDENT PLACE State b OTHER CLAIM ID Designated by NUCC 9b reserved for NUCC Use YES NO TN xx b Other claim ID c RESERVED FOR NUCCUSE c OTHERACOIDENTI c INSURANCE PLAN NAME OR PROGRA AM NAME 9c reserved for NUCC Use YES NO c Insurance Plan Name d INSURANCE PLAN NAME OR PROGRAM NAME d CLAIM CODES Designated by NUCC d is THERE ANOTHER HEALTH BENERT PLAN 9d Ins Plan Name or Program Name d Claim codes YES NO yes complete items 9 9a and 9d READ BACK OF FORU BEI FORE COMPLETING A SIGNING THIS FORM 13 INSUREDS OR AUTHORIZED PERSONS SIGNATURE authorize 12 PATIENTS OR AUTHORIZED PERSONS SIGNATURE lauthorize the release of anymedical or other information recessary payment of medical benefits to the undersigned physidan or suppler tor to process this claim alsorequest pa yment of government benefts ether tomyselfor to the party shoaccepts assignment services described below below SIGNED 12 signiture here  DATE 13 signiture here SIGNED 14 DATE OF CURRENT ILLINESS INJURY or PREGNANCY LMP 15 OTHER DATE MM DO YY MM DO YY 16 DATES IENT UNABLE TOWORK IN CURBENT OCCUPATION  QUAL 123456 QUAL QL  FROM  TO   NAME OF REFERRING PROMDER OR OTHER SOURCE a a 21215464 18 HOSPITALIZATION DATES RELATED TO CURRENT SERVICES MM DO YY MM DD YY Smith  b NPI 251987531 FROM  TO  19 ADDITIONAL INFORMATION Designated by NJCC 20 OUTSIDELAB CHARGES additional claim information YES NO 0000 21 DIAGNOSIS OR NATURE OF LLNESS OR INJURY Pelate AL bse woelne below 24E ICD Ind E 22 RESU JEMISSION CODE ORIGINAL REF NO A a525 B b52500 C c525 D d54554 ABC123 origrefno123456 E e52220 F f52422 a g45420 H h54556 23 PRIOR AUTHORIZA TION NUMBER I i54122 J 54221 K k654 L 158556 priorauth123465 24 A DATES OF SERVICE B c D PROCEDURES SERVICES OR SUPPLIES E H L J From To PLACEOF Explain Unusual Craunstances DIAGNOSIS DAYS ID RENDERING MM DD YY MM DD YY SERVICE EMG OPTHCPCS MODIFIER PONTER SCHARGES UNTS Ple QUAL PROVIDER  1   21 1C 99201  1 12500 1 H NPI 51987555 c 2   A33 2C 400  2 000 2 H NPI 251234567 3  si   44 3C 640 31 32 33 34 3 50 1 3 NPI 252121212 4   44 4C 99444 41 42 43 44 4 4040 4 H NPI 254141414 5   12  55 5C 451 51 52 53 54 5 5500 5 H NPI 255454542 6  12   13  66 6C 478 61 62 63 64 6 6600 6 H NPI 256565656 25 FEDERAL TAX LD NUMBER SGN BN 26 PATIENTS ACCOUNT NO 27 29 TOTAL CHARGE 29 AMOUNT PAID 30 Revd to  471234567 AC549879 YES NO  396 90  20000 31 SIGNATURE OF PHYSICIAN OR SUPPLIER 32 SERVICE FACILITY LOCATION INFORMATION 33 BILLING PROMIDER INFO PH INCLUDING DEGREES OR CREDENTIALS 800 12222 that the statements on thereverse Facility name Facility name Billing Provider Info apply this bil and are made part thereof 2 Facility Road 33 Billing Provider Street 31 signture of physician   2820 SIGNED DATE a 32216649a 32245165b a 33216649a 33245165b NUCC instruction Manual available at  PLEASE PRINT OR TYPE APPROVED CMB0938 97 FORM 1500 12
CMS1500,ABC Insurance 123 Main Street HETH INSURANCE CLAIM FORM City ST 12345 BY NATION UNIFORM CLAM PCA III MEDICARE THOCARE CHAMPVA THPLAN use is INSURADSID allen 1 L Number CA on spay  NAME 3 80 SEX 4 NAME Last Name Name Mode inda    5 PATIENTS ADDRESS Served  PATIENT UPEO  INSUREDS ADCRESS ons Street  Other  CITY STATE  RESERVED FOR NUCC USE CITY STATE Mycity  Mycity  noude become TELEPHONE Ave Code 12345    OTHER ASURIUS NAME  Name Name Mode waul 10 SPATIENTS CONON TO 11 GAOW ORIGA NUMBER ABCDE123  OTHER on NUMDER a EMPLOYVENTI Current or  INSURE  DATE or SEX YES NO  M F a RESERVED FOR NUCCUSE b AUTO ACCADENT PLACE Sure  o Designated by MUCC VES NO c RESERVEDOR NUCC USC  DENT o INSURANCE PLANNAME on YES NO d INSURANCE PEAN NAME on NAME 106  NUCC THERE PLAN YES NO Pyrs compane term 9 a was READ BACK or FORM BEFORE COMPLE TING  SIGNING THIS FORM 13 INSURLOS OR AUTHORIZED PERSONS seas FUE 12 PATIENTS on AUTHORIZED PERSON Pe release of any other interration necessary payment of medical No the andersy ned physician or supgler for as am  required pryment of givenes benefits other as cells to spenty  accepts servicesdesoribedbere beine SIGNED Signature on file DATE  SINED Signature on file r 14 or or am 15 OTHERDATE MM  YY  DATES xt male J9 WORK our OU FROM 17 NAME or nor PROVIDER OR OTHER sounce 176 18 HOSAT DATE LATED TO 175 NP FROM 10 ADD TON CLAN FON NICC 20 OUTSOD LABS SCHWROCS Yrs NO  21 OR NATURE or Polate  toserves and below 248 com 22 DIVISION A a c D c  a 23 PRICA AUTHORIZZATION NUMBER     KL 24 A SURVICE a c 0 PROCEDURES services e   From To o RENDERING um DO YY MM DO YY M 1 MODIFIER PONTER or am o NM 1 12345678    12345 UI 12 1  4 NPI OWETYUI 2 NM 12345678    12345 UI 3 50  4 MP OWETYUI 3 MP 4 NF 5 NPY 6 N 25 FEDER TAXIC SIN EN 26 PATIENTS ACCOUNT NO 27 25 TOT CHARGA 29 AMOUNT PAO  MAKE Use 234678219 ABC12345  NO  150  0 0 31 SCHATURE OF PHYSO on 32 SERVICE FACIUTY LOCATION TION 33 BUING ACCURANCE DEGREES 555 5789012 that the statements on ABC Clinic apply to this bil MD are made pert   City  DATE a 12345678 123456778 AKEUFLKJDSLFI NUCC Instruction Manual available at  LEASE PRINT OR TYPE APPROVED OMB OICE 1197 FORM 15 0212
CMS1500,Insurance Company Name Address HEALTH INSURANCE CLAIM FORM Payer ID COMMITTER CHOS 022 PICA  MIDICAME MEDICHE PRICATE or onen 18 D for   1 PLAN MG address Studentle 00 on ABC4444333 2 PARES  Name First Name  10 4 Test Name   x    Shom     any STATE a any STATE Anycity  Anycity     9  Name 10 to   CM  12345   MPLOIMENTS Demate America  require OF CHICH  189 NO  M Fl  PESERMED TORMACCUS a AUTOACCEINT PLACE devele  onem CLAMIO NO C RESEMVER NACCUSE c E INSURANCE PLAN OR AND NO di INDURANCE PLAN NISE on NAME 100   THERE ANOTHER HEALTH PLAY ves NO Eyes a was THAD BACK or FORN  one comme The  warom o CA AUTHORECO  12 PATIENTO OR SOBATURE ce fes or   persons of  the I for Ancien die  not present the    using storage and ted   BONED Signature on file DATE  GONED Signature on file 14  DUARENT a LMP 15 CHERBATE as DO TY MM am 00 YY  19 WORK IN M 00 no are than to 17 OF PROME OR OTHER SOURCE 17 18 MOSP gation DECEMBER services 06  lew or 110 PROM TO 19 CLAM 10 try 20 sownes ves NO 21 di Alume OF OR AL CAN 0 22 12F NO  F4320  c  c 28    a      2 A a servica e c D services  E  9   4 Foom To PAT 1 Epic have   MM DO w  DD YY trives  MORE PONTER SCHARGED are ne 201  1    90637 A 125 00  N 7 2 NA 3 wh 4 which 5 NA 6 at 25 GBN CN  PARENTS ACCOUNT NO 27 29 TOTAL ONCIGE 20 SMICUNT PAD 11 ex NUCCUME 99999999 65 NO  12500 9 2500 21 OF on SUPPUER  DEFINE MOURY LOCATION  PROVIDER 04 the of CREDING   that de Your Practice Name Here Therapes Name   Asdress Address Therapist Name City State  State Zip  some      NUCC instruction Manual avalable a nece ag PLEASE PRINT OR TYPE CMB 2 Clear Forme
CMS1500,1500 HEALTH INSURANCE CLAIM FORM AMPROVED BY  ncA 1 MIDICAD good on NUMBLA For   a  nx in ay  sand  DE SSNar  ARD    NAME Name time Make   SEX  NAME  Name Name Mode ints    Same as Patient ADCPESS one 6 PATENT TO INSURED 1 ACCPESS Na than  the Other same cmy STATE  PATENT STATUG GITY STATE  sage  Other 20   Ful Taxe  Student Student    OTHER NAME and TEAR Name   PATHINTY  CONDITION RELATED TO  POUCY on FECA 999999  OTHER POLION OR GROUP NUMBER  EMPLOYMENTY or Previous  of mith SEX in Y88 NO  u   DATE or  AUTO ACCIDENTY use DO YY PLACE claim a EMPLOYERS OR SCHOCL NAME M  YEA NO Doe Incorporated 6 EMPLOYERS TEAMEL OR NAME 0 OTHER ACCIDENT  PLAN NAME om ves NO Big Insurance Co  OR 100 nom LOCAL use di  THERE WEAL PLANT VES NO a you man to axe and  od TIEAD BACK or form SEFORE COMPLETING  THES rom cn PERSONS 12 PATENTS on PERSONS the   any the persons  the markgand et measure  no can as come  DE  The cony were aconces describos  Signature on File Signature on File DATE SONIO Y 14 GATE or GLNESS on 15 if PATENT MAS MAQ SAVE on SIMILAR 65 DATES TANT PADLE WORK om GIVE PRST DATE uns DO    7 THANK or movision on sounce 17 18 gan st DARESPELATED 10 Y see 00 on NM FROM to 10 FOR ICCAL USE 20 SCHARCES YES NO 21 CHONCES CRINATURE OF OR NURY itens 12  24E 22 VERICAID coce CRICIAN FER NO  90210 x 23 PRICH ITHORIZATION 24A OF SERVICE e C D PROCEDUPES OR APRLES e    a From Te mate o MM DO YV DO YY am  and  1 07 18  99990 1 12500 1 un 2 07    10  99990 1 12500 1 MA 3    99999 1 9999 1 in 4 MM 5 M 6 M is FEDEPAL TAX LD NAMSEN EN 25 PATIENT  ACCOUNT NO 29 28 TOTAL 29 PAD 30 BALANCE DUE 999999999 x  NO  34999  2500 32499 21 or on sumum 12 service FACALTY LOCATIONI  INCLUDINO on CHEDENTIALE John Doe Therapy  a carry the the  the   apply 10 the 01   make part    SERVICE DATE 999999 999999 NUCC Instruction Manual available at  APPROVED OMB 09380999 FORM CMS1500
"""


@task_group
def document_classifier_workflow():
    # [START howto_operator_create_document_classifier]
    create_document_classifier = ComprehendCreateDocumentClassifierOperator(
        task_id="create_document_classifier",
        document_classifier_name="custom-insurance-doc-classifier",
        input_data_config=input_data_configurations,
        output_data_config=output_data_configurations,
        mode="MULTI_CLASS",
        data_access_role_arn=test_context[ROLE_ARN_KEY],
        language_code="en",
        document_classifier_kwargs=document_classifier_kwargs
    )
    # [END howto_operator_create_document_classifier]
    create_document_classifier.wait_for_completion = False

    # [START howto_sensor_create_document_classifier]
    await_create_document_classifier = ComprehendCreateDocumentClassifierCompletedSensor(
        task_id="await_create_document_classifier", document_classifier_arn=create_document_classifier.output
    )
    # [END howto_sensor_create_document_classifier]

    @task
    def delete_classifier(document_classifier_arn: str):
        ComprehendHook().conn.delete_document_classifier(DocumentClassifierArn=document_classifier_arn)

    chain(create_document_classifier, await_create_document_classifier, delete_classifier(create_document_classifier.output))


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    bucket_name = f"{env_id}-comprehend-document-classifier"
    input_data_configurations = {
        "S3Uri": f"s3://{bucket_name}/{INPUT_S3_KEY}",
        'DataFormat': 'COMPREHEND_CSV',
    }
    output_data_configurations = {"S3Uri": f"s3://{bucket_name}/output/"}
    document_classifier_kwargs = {
        "VersionName": "v1"
    }

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    upload_sample_data = S3CreateObjectOperator(
        task_id="upload_sample_data",
        s3_bucket=bucket_name,
        s3_key=INPUT_S3_KEY,
        data=json.dumps(SAMPLE_DATA),
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )


    @task.branch
    def execute_document_classifier_or_skip():
        return end_process.task_id if SKIP_DOCUMENT_CLASSIFIER else test_context.task_id


    end_process = EmptyOperator(task_id="end_process")

    run_or_skip = execute_document_classifier_or_skip()

    chain(run_or_skip, Label("Create document classifier takes longer - skipping system test"), end_process)
    chain(
        run_or_skip,
        test_context,
        create_bucket,
        upload_sample_data,
        # TEST BODY
        document_classifier_workflow(),
        # TEST TEARDOWN
        delete_bucket,
        end_process
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
