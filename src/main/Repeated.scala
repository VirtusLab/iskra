package org.virtuslab.iskra

type Repeated[A] =
  A
  | (A, A)
  | (A, A, A)
  | (A, A, A, A)
  | (A, A, A, A, A)
  | (A, A, A, A, A, A)
  | (A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A)
  | (A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A, A) // 22 is maximal arity
