export interface IUser {
  id: string;
  email: string;
  name: string;
  createdAt: Date;
  updatedAt: Date;
}
//  Pick-ə siyahı verərkən belə deyirik: 👉
//  "Mənə IUser-dən həm 'id' açarını, həm də 'email' açarını seçib gətir."
export interface IAuthUser extends Pick<IUser, 'id' | 'email'> {
  roles: string[];
}
