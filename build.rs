fn main(){
    #[cfg(windows)]//claro voce esta no office querido, nao e?
    {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("./imagens/icon.ico"); 
        res.compile().unwrap();
    }
}